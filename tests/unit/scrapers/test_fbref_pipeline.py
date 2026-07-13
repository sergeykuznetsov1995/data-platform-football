from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest

from scrapers.fbref.control.models import (
    BudgetReservation,
    CohortTarget,
    FrontierTarget,
    ObservationLease,
    TargetLease,
    ThrottleSlot,
)
from scrapers.fbref.control.store import BudgetExceeded
from scrapers.fbref.fetcher import FETCHER_VERSION, FetchError, FetchResponse
from scrapers.fbref.page_document import PAGE_DOCUMENT_VERSION
from scrapers.fbref.pipeline import (
    FBrefPipeline,
    FETCH_LEASE_SECONDS,
    FetchWaveError,
    ParseWaveError,
    PipelineSettings,
    RunValidationError,
    SENTINEL_COMPETITIONS,
    frontier_target,
    page_target_from_link,
)
from scrapers.fbref.discovery import (
    DISCOVERY_PARSER_VERSION,
    DiscoveredPageLink,
)
from scrapers.fbref.raw_store import (
    PageTarget,
    RawPageStore,
    competition_index_target,
    match_page_target,
    season_page_target,
)
from scrapers.fbref.typed_bronze import TYPED_BRONZE_PARSER_VERSION


NOW = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)


def _complete_sentinel_coverage():
    return {
        name: {
            "published": True,
            "competition_id": str(index),
            "gender": "male",
            "eligibility": "eligible",
        }
        for index, name in enumerate(SENTINEL_COMPETITIONS, start=1)
    }


class FakeWriter:
    def __init__(self):
        self.pages = []

    def persist_page(self, page, **kwargs):
        self.pages.append((page, kwargs))
        return {"cells": 0, "tables": len(page.tables), "manifest": 1}


class FakeTypedWriter:
    def __init__(self, *, fail=False, events=None):
        self.fail = fail
        self.calls = []
        self.events = events

    def persist_schedule(self, parsed, **kwargs):
        self.calls.append(("schedule", parsed, kwargs))
        if self.events is not None:
            self.events.append("typed_write:schedule")
        if self.fail:
            raise RuntimeError("typed persistence failed")
        return {"schedule": parsed.row_count}

    def persist_season_stats(self, parsed, **kwargs):
        self.calls.append(("season", parsed, kwargs))
        return {
            name: dataset.row_count
            for name, dataset in parsed.items()
            if dataset.status.value == "available"
        }

    def persist_match(self, parsed, **kwargs):
        self.calls.append(("match", parsed, kwargs))
        return {
            name: dataset.row_count
            for name, dataset in parsed.datasets.items()
            if dataset.status.value == "available"
        }


class FakeTypedAdapter:
    def __init__(self, writer):
        self.writer = writer


class FakeControl:
    def __init__(self, raw_store=None):
        self.raw_store = raw_store
        self.events = []
        self.frontier = {}
        self.fetches = []
        self.registry = {}
        self.seasons = []
        self.manifests = []
        self.observations = {}
        self.completed = []
        self.failed = []
        self.snapshots = []
        self.reservations = []
        self.settlements = []
        self.session_metrics = []
        self.heartbeats = []
        self.claim_calls = []
        self.run = {
            "run_type": "current",
            "status": "succeeded",
            "request_limit": 25,
            "byte_limit": 25 * 1024 * 1024,
            "requests_used": 0,
            "bytes_used": 0,
            "requests_reserved": 0,
            "bytes_reserved": 0,
        }

    def get_run(self, run_id):
        return dict(self.run)

    def create_due_run_cohort(self, run_id, *, page_kinds, limit):
        self.events.append("cohort")
        return [
            CohortTarget(
                target_id="fbref:competition_index:all",
                logical_refresh_id=str(uuid.UUID(int=2)),
                ordinal=0,
            )
        ][:limit]

    def claim_targets(
        self,
        run_id,
        worker_id,
        *,
        limit,
        lease_seconds,
        page_kinds=None,
        refresh_policies=None,
    ):
        self.events.append("claim")
        self.claim_calls.append({
            "limit": limit,
            "lease_seconds": lease_seconds,
            "page_kinds": page_kinds,
            "refresh_policies": refresh_policies,
        })
        return [
            TargetLease(
                attempt_id=str(uuid.UUID(int=3)),
                run_id=run_id,
                target_id="fbref:competition_index:all",
                logical_refresh_id=str(uuid.UUID(int=2)),
                canonical_url="https://fbref.com/en/comps/",
                page_kind="competition_index",
                source_ids={"competition_index": "all"},
                claim_token=str(uuid.UUID(int=4)),
                lease_epoch=1,
                attempt_number=1,
                leased_by=worker_id,
                lease_expires_at=NOW + timedelta(minutes=10),
            )
        ][:limit]

    def reserve_budget(self, *args, **kwargs):
        self.events.append("reserve")
        self.reservations.append((args, kwargs))
        return BudgetReservation(
            reservation_id=str(uuid.UUID(int=5)),
            run_id=args[0],
            logical_refresh_id=args[1],
            requests_reserved=kwargs["requests"],
            bytes_reserved=kwargs["bytes_"],
            status="reserved",
        )

    def bind_reservation(self, lease, reservation_id):
        self.events.append("bind")

    def heartbeat(self, lease, *, lease_seconds):
        self.events.append(f"heartbeat:{lease.target_id}")
        self.heartbeats.append((lease, lease_seconds))
        return NOW + timedelta(seconds=lease_seconds)

    def reserve_domain_slot(self, *args, **kwargs):
        self.events.append("throttle")
        return ThrottleSlot(
            domain="fbref.com",
            slot_token=str(uuid.UUID(int=6)),
            lease_epoch=1,
            scheduled_at=NOW,
        )

    def open_clearance_session(self, **kwargs):
        self.events.append("session_open")
        return str(uuid.UUID(int=7))

    def get_frontier_target(self, target_id):
        return self.frontier.get(target_id)

    @contextmanager
    def guard_latest_content(
        self, target_id, content_hash, logical_refresh_id
    ):
        self.events.append(f"content_guard:{target_id}")
        frontier = self.frontier.get(target_id) or {}
        try:
            if not frontier or frontier.get("state", "fetched") == "leased":
                yield None
            else:
                yield (
                    frontier.get("last_content_hash") == content_hash
                    and frontier.get(
                        "last_logical_refresh_id", logical_refresh_id
                    )
                    == logical_refresh_id
                )
        finally:
            self.events.append(f"content_guard_exit:{target_id}")

    def settle_budget(self, reservation_id, **kwargs):
        self.events.append("settle")
        self.settlements.append((reservation_id, kwargs))
        self.run["requests_used"] += kwargs["requests_used"]
        self.run["bytes_used"] += kwargs["bytes_used"]

    def record_session_metrics(self, session_id, **kwargs):
        self.events.append("metrics")
        self.session_metrics.append((session_id, kwargs))

    def complete_fetch(self, lease, **kwargs):
        assert self.raw_store is None or self.raw_store.has_fetch(
            lease.logical_refresh_id
        )
        self.events.append("complete")
        self.completed.append((lease, kwargs))

    def fail_fetch(self, lease, **kwargs):
        self.events.append("fail")
        self.failed.append((lease, kwargs))

    def close_clearance_session(self, session_id, **kwargs):
        self.events.append("session_close")

    def list_run_fetches(
        self,
        run_id,
        *,
        page_kinds,
        limit,
        only_unparsed=False,
        parser_version=None,
        typed_parser_version=None,
        stateful_parser_version=None,
    ):
        rows = [
            item for item in self.fetches if item["page_kind"] in page_kinds
        ]
        if only_unparsed and typed_parser_version is not None:
            rows = [
                item for item in rows
                if self.observations.get((
                    str(item["logical_refresh_id"]),
                    str(parser_version),
                    str(typed_parser_version),
                    str(stateful_parser_version),
                ), {}).get("status") != "succeeded"
            ]
        return rows[:limit]

    def list_replay_fetches(
        self,
        run_id,
        *,
        parser_version,
        typed_parser_version=None,
        stateful_parser_version=None,
        page_kinds=None,
        limit,
    ):
        self.events.append(
            f"replay:{parser_version}:{typed_parser_version or 'none'}"
        )
        return self.list_run_fetches(
            run_id,
            page_kinds=page_kinds,
            limit=limit,
            only_unparsed=True,
            parser_version=parser_version,
            typed_parser_version=typed_parser_version,
            stateful_parser_version=stateful_parser_version,
        )

    def claim_observation_processing(self, **kwargs):
        key = (
            str(kwargs["logical_refresh_id"]),
            str(kwargs["parser_version"]),
            str(kwargs["typed_parser_version"]),
            str(kwargs["stateful_parser_version"]),
        )
        row = self.observations.get(key)
        if row is not None and row["status"] in {"processing", "succeeded"}:
            return None
        token = str(uuid.uuid4())
        self.observations[key] = {
            "status": "processing",
            "target_id": str(kwargs["target_id"]),
            "content_hash": str(kwargs["content_hash"]),
            "claim_token": token,
        }
        return ObservationLease(
            logical_refresh_id=key[0],
            target_id=str(kwargs["target_id"]),
            content_hash=str(kwargs["content_hash"]),
            parser_version=key[1],
            typed_parser_version=key[2],
            stateful_parser_version=key[3],
            claim_token=token,
            lease_expires_at=NOW + timedelta(
                seconds=int(kwargs["lease_seconds"])
            ),
        )

    def complete_observation_processing(self, lease, **kwargs):
        key = (
            lease.logical_refresh_id,
            lease.parser_version,
            lease.typed_parser_version,
            lease.stateful_parser_version,
        )
        self.observations[key].update(status="succeeded", **kwargs)
        self.events.append(f"observation_complete:{lease.target_id}")

    def fail_observation_processing(self, lease, **kwargs):
        key = (
            lease.logical_refresh_id,
            lease.parser_version,
            lease.typed_parser_version,
            lease.stateful_parser_version,
        )
        self.observations[key].update(status="failed", **kwargs)

    def list_backfill_seasons(self, *, limit):
        rows = []
        for entry in self.seasons:
            if entry.is_current:
                continue
            target = season_page_target(
                entry.competition_id,
                entry.season_id,
                entry.canonical_url,
            )
            frontier = self.frontier.get(target.target_id, {})
            completed = (
                frontier.get("refresh_policy") == "historical_once"
                and frontier.get("state") == "fetched"
                and frontier.get("next_fetch_at") is None
            )
            if completed:
                continue
            rows.append(
                {
                    "competition_id": entry.competition_id,
                    "season_id": entry.season_id,
                    "canonical_url": entry.canonical_url,
                    "is_current": entry.is_current,
                }
            )
        return rows[:limit]

    def record_dataset_manifest(self, **kwargs):
        self.manifests.append(kwargs)

    def create_registry_snapshot(self, **kwargs):
        self.events.append("snapshot")
        self.snapshots.append(dict(kwargs))
        return str(kwargs.get("snapshot_id") or uuid.uuid4())

    def reconcile_competitions(self, snapshot_id, entries):
        for entry in entries:
            self.registry[entry.competition_id] = {
                "competition_id": entry.competition_id,
                "canonical_url": entry.canonical_url,
                "name": entry.name,
                "gender": entry.gender,
                "classification": entry.classification,
                "metadata": dict(entry.metadata),
            }
        return {}

    def eligible_competitions(self):
        return [
            row for row in self.registry.values() if row["gender"] == "male"
        ]

    def reconcile_seasons(self, snapshot_id, competition_id, entries):
        self.seasons.extend(entries)
        return {}

    def upsert_frontier_target(self, target):
        self.events.append(f"frontier_upsert:{target.target_id}")
        previous = self.frontier.get(target.target_id, {})
        one_shot = {"historical_once", "current_completed_once"}
        previous_policy = previous.get("refresh_policy")
        incoming_policy = target.refresh_policy
        keep_recurring = (
            previous_policy is not None
            and previous_policy not in one_shot
            and incoming_policy == "historical_once"
            and target.page_kind in {"player", "squad"}
        )
        upgrade_to_recurring = (
            previous_policy in one_shot
            and incoming_policy not in one_shot
            and previous.get("next_fetch_at") is None
        )
        lifecycle_transition = (
            previous_policy is not None
            and previous_policy not in one_shot
            and (
                target.page_kind == "match"
                and incoming_policy == "current_completed_once"
                or target.page_kind == "season"
                and incoming_policy == "historical_once"
            )
        )
        self.frontier[target.target_id] = {
            "target_id": target.target_id,
            "page_kind": target.page_kind,
            "canonical_url": target.canonical_url,
            "source_ids": dict(target.source_ids),
            "refresh_policy": (
                previous_policy if keep_recurring else incoming_policy
            ),
            "state": previous.get("state", "queued"),
            "next_fetch_at": (
                target.next_fetch_at
                if target.next_fetch_at is not None
                else NOW if upgrade_to_recurring or lifecycle_transition
                else previous.get("next_fetch_at")
            ),
        }

    def create_run_cohort(self, run_id, cohort):
        self.events.append(f"explicit_cohort:{len(cohort)}")
        return len(cohort)

    def get_run_summary(self, run_id, **_versions):
        return {
            **self.run,
            "budget_exceeded": False,
            "target_counts": {"succeeded": 1},
            "dataset_validation_counts": {"succeeded": 1},
            "sentinel_coverage": _complete_sentinel_coverage(),
        }

    def finish_run(self, run_id, *, succeeded):
        self.events.append(f"finish:{succeeded}")


class FakeFetcher:
    def __init__(self, events, body, *, http_requests=1):
        self.events = events
        self.body = body
        self.http_requests = http_requests

    def __enter__(self):
        self.events.append("fetcher_enter")
        return self

    def __exit__(self, *args):
        self.events.append("fetcher_exit")

    def fetch(self, url, **kwargs):
        self.events.append("http")
        return FetchResponse(
            url=url,
            status_code=200,
            body=self.body,
            headers={"etag": '"v1"'},
            latency_ms=10,
            http_wire_bytes=len(self.body) + 120,
            decoded_html_bytes=len(self.body),
            http_requests=self.http_requests,
            http_status_history=(
                (500, 200)
                if self.http_requests == 2
                else (200,) * self.http_requests
            ),
            browser_document_bytes=500,
            browser_asset_bytes=100,
            browser_requests=1,
            browser_bootstrap_attempts=1,
        )


class FakeFailingFetcher:
    def __init__(self, events):
        self.events = events

    def __enter__(self):
        self.events.append("fetcher_enter")
        return self

    def __exit__(self, *args):
        self.events.append("fetcher_exit")

    def fetch(self, url, **kwargs):
        self.events.append("http")
        raise FetchError(
            "redacted status_history=500,500 body_sha256=abc",
            error_class="http_status",
            http_status=500,
            wire_bytes=303,
            browser_document_bytes=500,
            browser_asset_bytes=100,
            browser_requests=3,
            browser_bootstrap_attempts=2,
            browser_unobserved_bytes=400,
            target_requests=2,
            http_status_history=(500, 500),
            latency_ms=321,
        )


def _raw_store(tmp_path):
    return RawPageStore.from_uri(tmp_path.as_uri())


def _settings(run_type="current"):
    return PipelineSettings(
        run_type=run_type,
        request_limit=25,
        byte_limit=25 * 1024 * 1024,
        shard_size=4,
        request_reservation_bytes=4 * 1024 * 1024,
        domain_interval_seconds=0.01,
    )


def test_settings_cannot_underreserve_bounded_status_retry_requests():
    with pytest.raises(ValueError, match="cover both HTTP attempts"):
        PipelineSettings(target_request_reservation=1)


def _commit_for_parse(store, target, html):
    refresh = str(uuid.uuid4())
    record = store.commit_fetch(
        target,
        html.encode(),
        logical_refresh_id=refresh,
        attempt_id=str(uuid.uuid4()),
        http_status=200,
    )
    return refresh, record


def test_stats_subpages_have_distinct_canonical_target_identity():
    first = page_target_from_link(DiscoveredPageLink(
        page_kind="season_stats",
        canonical_url="https://fbref.com/en/comps/9/2025-2026/shooting/x",
        source_ids={
            "competition_id": "9",
            "season_id": "2025-2026",
            "stat_route": "shooting",
        },
    ))
    second = page_target_from_link(DiscoveredPageLink(
        page_kind="season_stats",
        canonical_url="https://fbref.com/en/comps/9/2025-2026/misc/x",
        source_ids={
            "competition_id": "9",
            "season_id": "2025-2026",
            "stat_route": "misc",
        },
    ))

    assert first.target_id != second.target_id
    assert first.canonical_url.endswith("/shooting/x")


def test_global_entity_target_has_stable_source_ids_across_contexts():
    first = page_target_from_link(DiscoveredPageLink(
        page_kind="player",
        canonical_url="https://fbref.com/en/players/1234abcd/Player",
        source_ids={
            "player_id": "1234abcd",
            "competition_id": "9",
            "season_id": "2025-2026",
        },
    ))
    second = page_target_from_link(DiscoveredPageLink(
        page_kind="player",
        canonical_url="https://fbref.com/en/players/1234abcd/Player",
        source_ids={
            "player_id": "1234abcd",
            "competition_id": "8",
            "season_id": "2024-2025",
            "squad_id": "wrong-squad",
        },
    ))

    assert first == second
    assert first.source_ids == {"player_id": "1234abcd"}


def test_current_and_historical_squad_urls_are_distinct_targets():
    current = page_target_from_link(DiscoveredPageLink(
        page_kind="squad",
        canonical_url="https://fbref.com/en/squads/abcd1234/Current-Team-Stats",
        source_ids={"squad_id": "abcd1234", "season_id": "2025-2026"},
    ))
    historical = page_target_from_link(DiscoveredPageLink(
        page_kind="squad",
        canonical_url=(
            "https://fbref.com/en/squads/abcd1234/2024-2025/Team-Stats"
        ),
        source_ids={"squad_id": "abcd1234", "season_id": "2024-2025"},
    ))

    assert current.target_id != historical.target_id
    assert current.source_ids != historical.source_ids
    assert current.source_ids["squad_id"] == historical.source_ids["squad_id"]
    assert set(current.source_ids) == {
        "squad_id", "squad_discriminator"
    }


def test_current_and_backfill_share_player_without_policy_downgrade():
    player = PageTarget(
        source="fbref",
        page_kind="player",
        target_id="fbref:player:1234abcd",
        canonical_url="https://fbref.com/en/players/1234abcd/Player",
        source_ids={"player_id": "1234abcd"},
    )
    control = FakeControl()
    control.upsert_frontier_target(frontier_target(player, historical=False))
    control.frontier[player.target_id].update(
        state="fetched", next_fetch_at=NOW + timedelta(days=30)
    )

    control.upsert_frontier_target(frontier_target(player, historical=True))

    assert control.frontier[player.target_id]["refresh_policy"] == "monthly"
    assert control.frontier[player.target_id]["next_fetch_at"] == (
        NOW + timedelta(days=30)
    )

    reverse = FakeControl()
    reverse.upsert_frontier_target(frontier_target(player, historical=True))
    reverse.frontier[player.target_id].update(
        state="fetched", next_fetch_at=None
    )
    reverse.upsert_frontier_target(frontier_target(player, historical=False))

    assert reverse.frontier[player.target_id]["refresh_policy"] == "monthly"
    assert reverse.frontier[player.target_id]["next_fetch_at"] == NOW

    match = match_page_target(
        "https://fbref.com/en/matches/abcdef12/source-match"
    )
    lifecycle = FakeControl()
    lifecycle.upsert_frontier_target(frontier_target(match, historical=False))
    lifecycle.frontier[match.target_id].update(
        state="fetched", next_fetch_at=NOW + timedelta(days=1)
    )
    current_completed = frontier_target(match, historical=False)
    lifecycle.upsert_frontier_target(FrontierTarget(
        **{
            **current_completed.__dict__,
            "refresh_policy": "current_completed_once",
        }
    ))
    assert lifecycle.frontier[match.target_id]["refresh_policy"] == (
        "current_completed_once"
    )
    assert lifecycle.frontier[match.target_id]["next_fetch_at"] == NOW

    season = season_page_target(
        "9", "2024-2025", "https://fbref.com/en/comps/9/2024-2025/x"
    )
    lifecycle.upsert_frontier_target(frontier_target(season, historical=False))
    lifecycle.frontier[season.target_id].update(
        state="fetched", next_fetch_at=NOW + timedelta(days=1)
    )
    lifecycle.upsert_frontier_target(frontier_target(season, historical=True))
    assert lifecycle.frontier[season.target_id]["refresh_policy"] == (
        "historical_once"
    )
    assert lifecycle.frontier[season.target_id]["next_fetch_at"] == NOW


def test_fetch_wave_reserves_budget_and_commits_raw_before_control(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    html = b"<html><table id='comps'><tr><td>x</td></tr></table></html>"
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda _: FakeFetcher(
            control.events, html, http_requests=2
        ),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        str(uuid.UUID(int=1)),
        worker_id="worker-1",
        page_kinds=["competition_index"],
        settings=_settings(),
    )

    assert result.fetched == 1
    assert raw.load_response(str(uuid.UUID(int=2)))[0] == html
    assert control.events.index("reserve") < control.events.index("http")
    assert control.events.index("http") < control.events.index("complete")
    assert control.events.index("settle") < control.events.index("complete")
    assert control.reservations[0][1]["requests"] == 22
    assert control.settlements[0][1]["requests_used"] == 3
    assert control.session_metrics[0][1]["http_requests"] == 2
    assert control.session_metrics[0][1]["browser_bootstrap_requests"] == 1
    assert control.session_metrics[0][1]["browser_bootstrap_attempts"] == 1
    _, raw_record = raw.load_response(str(uuid.UUID(int=2)))
    assert raw_record.http_requests == 2
    assert raw_record.http_status_history == (500, 200)
    assert raw_record.browser_bootstrap_attempts == 1
    assert control.completed[0][1]["http_request_count"] == 2
    assert control.completed[0][1]["http_status_history"] == (500, 200)
    assert result.requests == 3
    assert control.claim_calls[0]["lease_seconds"] == FETCH_LEASE_SECONDS


def test_fetch_wave_persists_retry_failure_evidence_and_exact_request_count(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda _: FakeFailingFetcher(control.events),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    with pytest.raises(FetchWaveError, match="http_status"):
        pipeline.fetch_wave(
            str(uuid.UUID(int=1)),
            worker_id="worker-1",
            page_kinds=["competition_index"],
            settings=_settings(),
        )

    assert control.reservations[0][1]["requests"] == 22
    assert control.settlements[0][1]["requests_used"] == 5
    assert control.run["requests_used"] == 5
    assert control.session_metrics[0][1]["http_requests"] == 2
    assert control.session_metrics[0][1]["browser_bootstrap_requests"] == 3
    assert control.session_metrics[0][1]["browser_bootstrap_attempts"] == 2
    assert control.session_metrics[0][1]["browser_unobserved_bytes"] == 400
    assert control.settlements[0][1]["bytes_used"] == 1303
    assert control.failed[0][1] == {
        "error_class": "http_status",
        "error_message": "redacted status_history=500,500 body_sha256=abc",
        "retry_delay_seconds": 60,
        "permanent": False,
        "http_status": 500,
        "http_request_count": 2,
        "http_status_history": (500, 500),
        "wire_bytes": 303,
        "provider_billed_bytes": None,
        "latency_ms": 321,
        "transport_version": FETCHER_VERSION,
        "session_version": str(uuid.UUID(int=7)),
    }


def test_fetch_wave_recovers_committed_raw_without_constructing_transport(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    target = competition_index_target()
    raw.commit_fetch(
        target,
        b"<html>committed</html>",
        logical_refresh_id=str(uuid.UUID(int=2)),
        attempt_id=str(uuid.UUID(int=99)),
        http_status=200,
    )

    def forbidden(_):
        raise AssertionError("transport constructed during raw recovery")

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=forbidden,
        clock=lambda: NOW,
    )
    result = pipeline.fetch_wave(
        str(uuid.UUID(int=1)),
        worker_id="worker-1",
        page_kinds=["competition_index"],
        settings=_settings(),
    )

    assert result.recovered_from_raw == 1
    assert result.requests == 0
    assert result.wire_bytes == 0
    assert result.decoded_html_bytes == 0
    assert result.browser_document_bytes == 0
    assert result.browser_asset_bytes == 0
    assert "reserve" not in control.events
    assert control.completed[0][1]["recovered_from_attempt_id"] == str(
        uuid.UUID(int=99)
    )


def test_sequential_wave_renews_current_and_waiting_leases(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    run_id = str(uuid.UUID(int=1))

    def lease(number, target_id, page_kind, canonical_url, source_ids):
        return TargetLease(
            attempt_id=str(uuid.UUID(int=30 + number)),
            run_id=run_id,
            target_id=target_id,
            logical_refresh_id=str(uuid.UUID(int=40 + number)),
            canonical_url=canonical_url,
            page_kind=page_kind,
            source_ids=source_ids,
            claim_token=str(uuid.UUID(int=50 + number)),
            lease_epoch=1,
            attempt_number=1,
            leased_by="worker-1",
            lease_expires_at=NOW + timedelta(minutes=10),
        )

    first = lease(
        1,
        "fbref:competition_index:all",
        "competition_index",
        "https://fbref.com/en/comps",
        {"competition_index": "all"},
    )
    second = lease(
        2,
        "fbref:competition:9",
        "competition",
        "https://fbref.com/en/comps/9/history/Premier-League-Seasons",
        {"competition_id": "9"},
    )
    control.claim_targets = lambda *args, **kwargs: [first, second]
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda _: FakeFetcher(
            control.events, b"<html>ok</html>"
        ),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        run_id,
        worker_id="worker-1",
        page_kinds=["competition_index", "competition"],
        settings=_settings(),
    )

    assert result.fetched == 2
    assert [item[0].target_id for item in control.heartbeats] == [
        first.target_id,
        second.target_id,
        second.target_id,
    ]
    assert all(
        seconds == FETCH_LEASE_SECONDS for _, seconds in control.heartbeats
    )


@pytest.mark.parametrize("raw_version", ["v1", "prior-v2"])
@pytest.mark.parametrize(
    ("run_type", "refresh_policy"),
    [
        ("backfill", "historical_once"),
        ("current", "current_completed_once"),
    ],
)
def test_one_shot_transition_refreshes_instead_of_adopting_prior_raw(
    tmp_path,
    raw_version,
    run_type,
    refresh_policy,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    run_id = str(uuid.UUID(int=1))
    refresh_id = str(uuid.UUID(int=20))
    target = match_page_target("a071faa8")
    stale = b"<html>stale-before-transition</html>"
    fresh = b"<html>fresh-final-state</html>"
    if raw_version == "v1":
        raw.store_html(target, stale.decode("utf-8"))
    else:
        raw.commit_fetch(
            target,
            stale,
            logical_refresh_id=str(uuid.UUID(int=19)),
            http_status=200,
        )
    lease = TargetLease(
        attempt_id=str(uuid.UUID(int=21)),
        run_id=run_id,
        target_id=target.target_id,
        logical_refresh_id=refresh_id,
        canonical_url=target.canonical_url,
        page_kind=target.page_kind,
        source_ids=dict(target.source_ids),
        claim_token=str(uuid.UUID(int=22)),
        lease_epoch=1,
        attempt_number=1,
        leased_by="worker-1",
        lease_expires_at=NOW + timedelta(minutes=10),
    )
    control.claim_targets = lambda *args, **kwargs: [lease]
    control.frontier[target.target_id] = {
        "refresh_policy": refresh_policy,
        "state": "queued",
    }

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda _: FakeFetcher(control.events, fresh),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )
    result = pipeline.fetch_wave(
        run_id,
        worker_id="worker-1",
        page_kinds=["match"],
        settings=_settings(run_type),
    )

    committed_body, committed = raw.load_fetch(refresh_id)
    assert committed_body == fresh
    assert committed.imported_from_manifest_key is None
    assert result.fetched == 1
    assert result.recovered_from_raw == 0
    assert result.requests == 2
    assert "reserve" in control.events
    assert "http" in control.events


def test_recurring_current_target_does_not_adopt_stale_v1_raw(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    target = competition_index_target()
    raw.store_html(target, "<html>stale</html>")
    control.frontier[target.target_id] = {
        "refresh_policy": "current_refresh",
        "state": "queued",
    }
    fresh = b"<html>fresh</html>"
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda _: FakeFetcher(control.events, fresh),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        str(uuid.UUID(int=1)),
        worker_id="worker-1",
        page_kinds=["competition_index"],
        settings=_settings("current"),
    )

    assert result.fetched == 1
    assert result.recovered_from_raw == 0
    assert raw.load_fetch(str(uuid.UUID(int=2)))[0] == fresh
    assert "reserve" in control.events
    assert "http" in control.events


def test_offline_index_parse_seeds_only_male_competitions(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    html = """
    <h2>Domestic Leagues</h2><table id="comps"><tbody>
      <tr><td data-stat="gender">M</td><th><a href="/en/comps/9/history/Premier-League-Seasons">Premier League</a></th></tr>
      <tr><td data-stat="gender">F</td><th><a href="/en/comps/189/history/Womens-Super-League-Seasons">Women's Super League</a></th></tr>
      <tr><td data-stat="gender">?</td><th><a href="/en/comps/x/history/Unknown-Seasons">Unknown Cup</a></th></tr>
    </tbody></table>
    """
    refresh, record = _commit_for_parse(raw, competition_index_target(), html)
    control.frontier[record.target_id] = {
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "source_ids": dict(record.source_ids),
        "state": "fetched",
        "last_content_hash": record.content_hash,
    }
    control.fetches = [{
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "logical_refresh_id": refresh,
    }]

    def forbidden(_):
        raise AssertionError("offline parse constructed transport")

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=forbidden,
    )
    result = pipeline.parse_wave(
        str(uuid.uuid4()),
        page_kinds=["competition_index"],
        settings=_settings(),
    )

    assert result.parsed == 1
    assert result.seeded == 1
    assert result.skipped_ineligible == 2
    child_kinds = [
        row["page_kind"] for key, row in control.frontier.items()
        if key != "fbref:competition_index:all"
    ]
    assert child_kinds == ["competition"]
    assert all(
        row["source_ids"].get("competition_id") == "9"
        for key, row in control.frontier.items()
        if key != "fbref:competition_index:all"
    )


def test_registry_snapshot_identity_is_stable_for_raw_retry(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    html = """
    <h2>Domestic Leagues</h2><table id="comps"><tbody>
      <tr><td data-stat="gender">M</td><th><a href="/en/comps/9/history/x">Premier League</a></th></tr>
    </tbody></table>
    """
    _, record = _commit_for_parse(raw, competition_index_target(), html)
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())
    run_id = str(uuid.uuid4())

    pipeline._parse_competition_index(run_id, html, record)
    pipeline._parse_competition_index(run_id, html, record)

    snapshot_ids = [item["snapshot_id"] for item in control.snapshots]
    assert snapshot_ids[0] == snapshot_ids[1]
    assert uuid.UUID(snapshot_ids[0]).version == 5


def test_current_history_parse_uses_exact_source_season_and_opaque_ids(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.registry["9"] = {
        "competition_id": "9",
        "canonical_url": "https://fbref.com/en/comps/9/history/x",
        "name": "Premier League",
        "gender": "male",
        "classification": "league:club",
        "metadata": {
            "source_section": "Domestic Leagues",
            "last_season": "Spring Edition",
        },
    }
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="competition",
        canonical_url="https://fbref.com/en/comps/9/history/x",
        source_ids={"competition_id": "9"},
    ))
    html = """
    <table id="seasons"><tbody>
      <tr><th data-stat="season"><a href="/en/comps/9/spring/source-owned-current">Spring Edition</a></th></tr>
      <tr><th data-stat="season"><a href="/en/comps/9/2024-2025/source-owned-old">2024-2025</a></th></tr>
    </tbody></table>
    """
    refresh, record = _commit_for_parse(raw, target, html)
    control.frontier[record.target_id] = {
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "source_ids": dict(record.source_ids),
        "state": "fetched",
        "last_content_hash": record.content_hash,
    }
    control.fetches = [{
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "logical_refresh_id": refresh,
    }]
    pipeline = FBrefPipeline(
        control, raw, generic_writer=FakeWriter(), fetcher_factory=lambda _: None
    )

    result = pipeline.parse_wave(
        str(uuid.uuid4()),
        page_kinds=["competition"],
        settings=_settings("current"),
    )

    season_targets = [
        row for row in control.frontier.values()
        if row["page_kind"] == "season"
    ]
    assert result.seeded == 1
    assert len(season_targets) == 1
    assert season_targets[0]["source_ids"]["season_id"] == "spring"
    assert season_targets[0]["canonical_url"].endswith(
        "/spring/source-owned-current"
    )
    assert {entry.is_current for entry in control.seasons} == {True, False}


def test_validation_fails_closed_on_partial_target_state(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.get_run_summary = lambda _, **__: {
        **control.run,
        "budget_exceeded": False,
        "target_counts": {"succeeded": 1, "retry": 1},
        "dataset_validation_counts": {"succeeded": 1},
    }
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(RunValidationError, match="incomplete_targets"):
        pipeline.validate_and_finish(str(uuid.uuid4()))
    assert "finish:False" in control.events


def test_validation_accepts_complete_eligible_sentinel_coverage(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    pipeline.validate_and_finish(str(uuid.uuid4()))

    assert "finish:True" in control.events


def test_validation_rejects_two_browser_bootstrap_attempts(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = control.get_run_summary(str(uuid.uuid4()))
    summary["session_metrics"] = {"max_bootstraps_per_session": 2}
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(
        RunValidationError,
        match="browser_bootstrap_exceeded_per_session",
    ):
        pipeline.validate_and_finish(str(uuid.uuid4()))

    assert "finish:False" in control.events


@pytest.mark.parametrize("run_type", ["current", "backfill", "replay"])
def test_validation_blocks_silver_with_pending_match_backlog(
    tmp_path, run_type
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.run["run_type"] = run_type
    summary = control.get_run_summary(str(uuid.uuid4()))
    summary["promotion_pending_match_count"] = 26
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(
        RunValidationError, match="promotion_pending_match_count=26"
    ):
        pipeline.validate_and_finish(
            str(uuid.uuid4()),
            replay_source_run_id=(
                str(uuid.uuid4()) if run_type == "replay" else None
            ),
        )

    assert "finish:False" in control.events


def test_validation_rejects_missing_sentinel_coverage(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = control.get_run_summary(str(uuid.uuid4()))
    summary["sentinel_coverage"].pop("World Cup")
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(RunValidationError, match="sentinel_coverage_missing"):
        pipeline.validate_and_finish(str(uuid.uuid4()))

    assert "finish:False" in control.events


def test_validation_rejects_ineligible_sentinel_coverage(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = control.get_run_summary(str(uuid.uuid4()))
    summary["sentinel_coverage"]["Premier League"]["eligibility"] = (
        "skipped_female"
    )
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(
        RunValidationError, match="sentinel_coverage_ineligible"
    ):
        pipeline.validate_and_finish(str(uuid.uuid4()))

    assert "finish:False" in control.events


def test_backfill_seeds_exact_next_historical_registry_url(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    from scrapers.fbref.control.models import SeasonRegistryEntry

    control.seasons = [
        SeasonRegistryEntry(
            competition_id="8",
            season_id="edition-42",
            canonical_url=(
                "https://fbref.com/en/comps/8/edition-42/source-owned"
            ),
            label="Edition 42",
            is_current=False,
        )
    ]
    pipeline = FBrefPipeline(
        control, raw, generic_writer=FakeWriter(), clock=lambda: NOW
    )

    result = pipeline.seed_historical_seasons(
        run_id=str(uuid.uuid4()), settings=_settings("backfill"), limit=4
    )

    assert result == {"seeded": 1, "auto_resume": True}
    seeded = next(iter(control.frontier.values()))
    assert seeded["canonical_url"].endswith("/edition-42/source-owned")
    assert seeded["refresh_policy"] == "historical_once"


def test_backfill_auto_resume_does_not_requeue_completed_historical_season(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    from scrapers.fbref.control.models import SeasonRegistryEntry

    completed = SeasonRegistryEntry(
        competition_id="8",
        season_id="edition-41",
        canonical_url="https://fbref.com/en/comps/8/edition-41/old",
        label="Edition 41",
        is_current=False,
    )
    pending = SeasonRegistryEntry(
        competition_id="8",
        season_id="edition-42",
        canonical_url="https://fbref.com/en/comps/8/edition-42/next",
        label="Edition 42",
        is_current=False,
    )
    control.seasons = [completed, pending]
    completed_target = season_page_target(
        completed.competition_id,
        completed.season_id,
        completed.canonical_url,
    )
    control.frontier[completed_target.target_id] = {
        "target_id": completed_target.target_id,
        "page_kind": "season",
        "canonical_url": completed_target.canonical_url,
        "source_ids": dict(completed_target.source_ids),
        "refresh_policy": "historical_once",
        "state": "fetched",
        "next_fetch_at": None,
    }
    pipeline = FBrefPipeline(
        control, raw, generic_writer=FakeWriter(), clock=lambda: NOW
    )

    result = pipeline.seed_historical_seasons(
        run_id=str(uuid.uuid4()), settings=_settings("backfill"), limit=1
    )

    assert result == {"seeded": 1, "auto_resume": True}
    assert control.frontier[completed_target.target_id]["state"] == "fetched"
    assert "fbref:season:8:edition-42" in control.frontier
    assert control.events[-1] == "explicit_cohort:1"


def test_backfill_seed_never_exceeds_worst_case_budget_capacity(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    from scrapers.fbref.control.models import SeasonRegistryEntry

    control.seasons = [
        SeasonRegistryEntry(
            competition_id="8",
            season_id=f"edition-{number}",
            canonical_url=f"https://fbref.com/en/comps/8/edition-{number}/x",
            label=f"Edition {number}",
            is_current=False,
        )
        for number in range(5)
    ]
    settings = _settings("backfill")
    pipeline = FBrefPipeline(
        control, raw, generic_writer=FakeWriter(), clock=lambda: NOW
    )

    result = pipeline.seed_historical_seasons(
        run_id=str(uuid.uuid4()), settings=settings, limit=8
    )

    expected_capacity = (
        settings.request_limit - settings.bootstrap_request_reservation
    ) // settings.target_request_reservation
    assert expected_capacity == 2
    assert result["seeded"] == expected_capacity
    assert control.events[-1] == f"explicit_cohort:{expected_capacity}"


def test_new_stateful_parser_replay_rebuilds_latest_raw_offline(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    html = """
    <h2>Domestic Leagues</h2><table id="comps"><tbody>
      <tr><td data-stat="gender">M</td><th><a href="/en/comps/9/history/x">Premier League</a></th></tr>
    </tbody></table>
    """
    refresh, record = _commit_for_parse(raw, competition_index_target(), html)
    control.frontier[record.target_id] = {
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "source_ids": dict(record.source_ids),
        "state": "fetched",
        "last_content_hash": record.content_hash,
    }
    control.observations[(
        refresh,
        PAGE_DOCUMENT_VERSION,
        TYPED_BRONZE_PARSER_VERSION,
        "old-discovery-parser",
    )] = {"status": "succeeded"}
    control.fetches = [{
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "logical_refresh_id": refresh,
    }]
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    result = pipeline.parse_wave(
        str(uuid.uuid4()),
        source_run_id=str(uuid.uuid4()),
        page_kinds=["competition_index"],
        settings=_settings("replay"),
    )

    assert result.parsed == 1
    assert result.seeded == 1
    assert set(control.registry) == {"9"}
    assert any(
        item.get("page_kind") == "competition"
        for item in control.frontier.values()
    )
    assert (
        refresh,
        PAGE_DOCUMENT_VERSION,
        TYPED_BRONZE_PARSER_VERSION,
        DISCOVERY_PARSER_VERSION,
    ) in control.observations
    assert any(event.startswith("replay:") for event in control.events)


@pytest.mark.parametrize(
    ("source_run", "error"),
    [
        (None, "replay_source_run_not_found"),
        (
            {"run_type": "replay", "status": "succeeded"},
            "replay_source_run_type_forbidden",
        ),
        (
            {"run_type": "current", "status": "pending"},
            "replay_source_run_not_terminal=pending",
        ),
        (
            {"run_type": "backfill", "status": "running"},
            "replay_source_run_not_terminal=running",
        ),
    ],
)
def test_replay_parse_rejects_invalid_or_live_source_run(
    tmp_path, source_run, error
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.get_run = lambda _: source_run
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(ParseWaveError, match=error):
        pipeline.parse_wave(
            str(uuid.uuid4()),
            source_run_id=str(uuid.uuid4()),
            page_kinds=["match"],
            settings=_settings("replay"),
        )

    assert not any(event.startswith("replay:") for event in control.events)


@pytest.mark.parametrize("status", ["failed", "succeeded", "cancelled"])
def test_replay_parse_accepts_terminal_non_replay_source_run(tmp_path, status):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.get_run = lambda _: {"run_type": "current", "status": status}
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    result = pipeline.parse_wave(
        str(uuid.uuid4()),
        source_run_id=str(uuid.uuid4()),
        page_kinds=["match"],
        settings=_settings("replay"),
    )

    assert result.parsed == 0
    assert any(event.startswith("replay:") for event in control.events)


def test_replay_validation_rejects_missing_source_run(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.run["run_type"] = "replay"
    control.get_run = lambda _: None
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(RunValidationError, match="replay_source_run_not_found"):
        pipeline.validate_and_finish(
            str(uuid.uuid4()), replay_source_run_id=str(uuid.uuid4())
        )

    assert "finish:False" in control.events


@pytest.mark.parametrize("status", ["failed", "succeeded", "cancelled"])
def test_replay_validation_accepts_terminal_source_run(tmp_path, status):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.run["run_type"] = "replay"
    control.get_run = lambda _: {"run_type": "backfill", "status": status}
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    pipeline.validate_and_finish(
        str(uuid.uuid4()), replay_source_run_id=str(uuid.uuid4())
    )

    assert "finish:True" in control.events


@pytest.mark.parametrize("latest", [False, True, None])
@pytest.mark.parametrize("run_type", ["current", "replay"])
def test_typed_promotion_is_guarded_for_live_and_replay(
    tmp_path, latest, run_type
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.registry["9"] = {
        "competition_id": "9",
        "canonical_url": "https://fbref.com/en/comps/9/history/x",
        "name": "Premier League",
        "gender": "male",
        "classification": "league:club",
        "metadata": {},
    }
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="schedule",
        canonical_url="https://fbref.com/en/comps/9/2025-2026/schedule/x",
        source_ids={"competition_id": "9", "season_id": "2025-2026"},
    ))
    html = """
    <table id="sched_all"><thead><tr>
      <th data-stat="date">Date</th><th data-stat="home_team">Home</th>
      <th data-stat="away_team">Away</th><th data-stat="score">Score</th>
      <th data-stat="match_report">Report</th>
    </tr></thead><tbody><tr>
      <th data-stat="date">2026-01-01</th>
      <td data-stat="home_team">A</td><td data-stat="away_team">B</td>
      <td data-stat="score">1–0</td>
      <td data-stat="match_report"><a href="/en/matches/abcdef12/x">Report</a></td>
    </tr></tbody></table>
    """
    refresh, record = _commit_for_parse(raw, target, html)
    control.frontier[target.target_id] = {
        "target_id": target.target_id,
        "page_kind": target.page_kind,
        "canonical_url": target.canonical_url,
        "source_ids": dict(target.source_ids),
        "refresh_policy": "six_hourly",
        "state": "leased" if latest is None else "fetched",
        "last_content_hash": (
            record.content_hash
            if latest is not False
            else "newer-content-hash"
        ),
    }
    control.fetches = [{
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "logical_refresh_id": refresh,
    }]
    typed_writer = FakeTypedWriter(events=control.events)
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        typed_adapter=FakeTypedAdapter(typed_writer),
    )

    if latest is None:
        with pytest.raises(ParseWaveError, match="TypedPromotionDeferred"):
            pipeline.parse_wave(
                str(uuid.uuid4()),
                source_run_id=(
                    str(uuid.uuid4()) if run_type == "replay" else None
                ),
                page_kinds=["schedule"],
                settings=_settings(run_type),
            )
        assert not any(
            item["dataset"] in {
                "typed:__stale_observation__",
                "typed:__complete__",
            }
            for item in control.manifests
        )
        assert f"content_guard:{target.target_id}" in control.events
        return

    result = pipeline.parse_wave(
        str(uuid.uuid4()),
        source_run_id=(str(uuid.uuid4()) if run_type == "replay" else None),
        page_kinds=["schedule"],
        settings=_settings(run_type),
    )

    if latest:
        assert result.typed_promoted == 1
        assert result.stale_typed_observations_skipped == 0
        assert [call[0] for call in typed_writer.calls] == ["schedule"]
        typed_datasets = [
            item["dataset"]
            for item in control.manifests
            if item["dataset"].startswith("typed:")
        ]
        assert typed_datasets[-1] == "typed:__complete__"
        assert typed_datasets.index("typed:schedule") < typed_datasets.index(
            "typed:__complete__"
        )
        if run_type == "current":
            guard = control.events.index(f"content_guard:{target.target_id}")
            typed = control.events.index("typed_write:schedule")
            stateful = next(
                index
                for index, event in enumerate(control.events)
                if event.startswith("frontier_upsert:fbref:match:")
            )
            complete = control.events.index(
                f"observation_complete:{target.target_id}"
            )
            unlocked = control.events.index(
                f"content_guard_exit:{target.target_id}"
            )
            assert guard < typed < stateful < complete < unlocked
    else:
        assert result.typed_promoted == 0
        assert result.stale_typed_observations_skipped == 1
        assert typed_writer.calls == []
        marker = next(
            item for item in control.manifests
            if item["dataset"] == "typed:__stale_observation__"
        )
        assert marker["persistence_status"] == "skipped"
        typed_datasets = [
            item["dataset"]
            for item in control.manifests
            if item["dataset"].startswith("typed:")
        ]
        assert typed_datasets[-1:] == ["typed:__stale_observation__"]
        key = (
            refresh,
            PAGE_DOCUMENT_VERSION,
            TYPED_BRONZE_PARSER_VERSION,
            DISCOVERY_PARSER_VERSION,
        )
        assert control.observations[key]["typed_status"] == "skipped"
    assert f"content_guard:{target.target_id}" in control.events


def test_same_hash_new_observation_and_a_b_a_each_promote_typed_once(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.registry["9"] = {
        "competition_id": "9",
        "canonical_url": "https://fbref.com/en/comps/9/history/x",
        "name": "Premier League",
        "gender": "male",
        "classification": "league:club",
        "metadata": {},
    }
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="schedule",
        canonical_url="https://fbref.com/en/comps/9/2025-2026/schedule/x",
        source_ids={"competition_id": "9", "season_id": "2025-2026"},
    ))
    html_a = """
    <table id="sched_all"><thead><tr>
      <th data-stat="date">Date</th><th data-stat="home_team">Home</th>
      <th data-stat="away_team">Away</th><th data-stat="score">Score</th>
    </tr></thead><tbody><tr><th data-stat="date">2026-01-01</th>
      <td data-stat="home_team">A</td><td data-stat="away_team">B</td>
      <td data-stat="score">1-0</td></tr></tbody></table>
    """
    html_b = html_a.replace("1-0", "2-0")
    observations = [
        _commit_for_parse(raw, target, html_a),
        _commit_for_parse(raw, target, html_a),
        _commit_for_parse(raw, target, html_b),
        _commit_for_parse(raw, target, html_a),
    ]
    assert observations[0][1].content_hash == observations[1][1].content_hash
    assert observations[0][1].content_hash == observations[3][1].content_hash
    typed_writer = FakeTypedWriter()
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        typed_adapter=FakeTypedAdapter(typed_writer),
    )

    for refresh, record in observations:
        control.frontier[target.target_id] = {
            "target_id": target.target_id,
            "state": "fetched",
            "last_content_hash": record.content_hash,
        }
        control.fetches = [{
            "target_id": record.target_id,
            "page_kind": record.page_kind,
            "logical_refresh_id": refresh,
            "content_hash": record.content_hash,
        }]
        result = pipeline.parse_wave(
            str(uuid.uuid4()),
            page_kinds=["schedule"],
            settings=_settings("current"),
        )
        assert result.parsed == 1

        repeated = pipeline.parse_wave(
            str(uuid.uuid4()),
            page_kinds=["schedule"],
            settings=_settings("current"),
        )
        assert repeated.parsed == 0

    assert [item[0] for item in typed_writer.calls] == ["schedule"] * 4
    assert len(control.observations) == 4


def test_typed_page_without_source_context_fails_observation(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    target = PageTarget(
        source="fbref",
        page_kind="match",
        target_id="fbref:match:abcdef12",
        canonical_url="https://fbref.com/en/matches/abcdef12/x",
        source_ids={"match_id": "abcdef12"},
    )
    refresh, record = _commit_for_parse(
        raw, target, '<table id="sched_all"></table>'
    )
    control.frontier[target.target_id] = {
        "target_id": target.target_id,
        "state": "fetched",
        "last_content_hash": record.content_hash,
    }
    control.fetches = [{
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "logical_refresh_id": refresh,
        "content_hash": record.content_hash,
    }]
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        typed_adapter=FakeTypedAdapter(FakeTypedWriter()),
    )

    with pytest.raises(ParseWaveError, match="source competition_id and season_id"):
        pipeline.parse_wave(
            str(uuid.uuid4()),
            page_kinds=["match"],
            settings=_settings("current"),
        )

    key = (
        refresh,
        PAGE_DOCUMENT_VERSION,
        TYPED_BRONZE_PARSER_VERSION,
        DISCOVERY_PARSER_VERSION,
    )
    assert control.observations[key]["status"] == "failed"
    assert not any(
        item["dataset"] == "typed:__complete__"
        for item in control.manifests
    )


def test_player_navigation_cannot_seed_match_and_deduplicates_matchlogs(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    target = PageTarget(
        source="fbref",
        page_kind="player",
        target_id="fbref:player:1234abcd",
        canonical_url="https://fbref.com/en/players/1234abcd/Player",
        source_ids={
            "player_id": "1234abcd",
            "competition_id": "wrong-comp",
            "season_id": "wrong-season",
        },
    )
    html = """
    <a href="/en/matches/aaaaaaaa/wrong-context">Navigation match</a>
    <a href="/en/players/1234abcd/matchlogs/">Logs root</a>
    <a href="/en/players/1234abcd/matchlogs/2025/summary/First-Slug">Logs</a>
    <a href="/en/players/1234abcd/matchlogs/2025/summary/Second-Slug">Duplicate</a>
    """
    refresh, record = _commit_for_parse(raw, target, html)
    control.frontier[target.target_id] = {
        "target_id": target.target_id,
        "page_kind": target.page_kind,
        "canonical_url": target.canonical_url,
        "source_ids": dict(target.source_ids),
        "refresh_policy": "monthly",
        "state": "fetched",
        "last_content_hash": record.content_hash,
    }
    control.fetches = [{
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "logical_refresh_id": refresh,
        "content_hash": record.content_hash,
    }]
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    result = pipeline.parse_wave(
        str(uuid.uuid4()),
        page_kinds=["player"],
        settings=_settings("current"),
    )

    children = [
        item for key, item in control.frontier.items()
        if key != target.target_id
    ]
    assert result.seeded == 1
    assert [item["page_kind"] for item in children] == ["matchlog"]
    assert children[0]["source_ids"] == {
        "player_id": "1234abcd",
        "matchlog_season_id": "2025",
        "matchlog_discriminator": "2025/summary",
    }


@pytest.mark.parametrize("typed_fails", [False, True])
def test_page_completion_marker_is_after_typed_schedule_persistence(
    tmp_path, typed_fails
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.registry["9"] = {
        "competition_id": "9",
        "canonical_url": "https://fbref.com/en/comps/9/history/x",
        "name": "Premier League",
        "gender": "male",
        "classification": "league:club",
        "metadata": {},
    }
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="schedule",
        canonical_url="https://fbref.com/en/comps/9/2025-2026/schedule/x",
        source_ids={"competition_id": "9", "season_id": "2025-2026"},
    ))
    html = """
    <table id="sched_all">
      <thead><tr><th data-stat="date">Date</th>
        <th data-stat="home_team">Home</th><th data-stat="away_team">Away</th>
        <th data-stat="score">Score</th><th data-stat="match_report">Report</th>
      </tr></thead><tbody><tr>
        <th data-stat="date">2026-01-01</th>
        <td data-stat="home_team">A</td><td data-stat="away_team">B</td>
        <td data-stat="score">1–0</td>
        <td data-stat="match_report"><a href="/en/matches/abcdef12/x">Report</a></td>
      </tr></tbody>
    </table>
    """
    refresh, record = _commit_for_parse(raw, target, html)
    control.frontier[target.target_id] = {
        "target_id": target.target_id,
        "state": "fetched",
        "last_content_hash": record.content_hash,
    }
    control.fetches = [{
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "logical_refresh_id": refresh,
    }]
    typed_writer = FakeTypedWriter(fail=typed_fails)
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        typed_adapter=FakeTypedAdapter(typed_writer),
    )

    if typed_fails:
        with pytest.raises(ParseWaveError, match="typed persistence failed"):
            pipeline.parse_wave(
                str(uuid.uuid4()),
                page_kinds=["schedule"],
                settings=_settings(),
            )
        page_marker = [
            item for item in control.manifests if item["dataset"] == "__page__"
        ][-1]
        assert page_marker["validation_status"] == "failed"
        assert not any(
            item["dataset"] == "typed:__complete__"
            for item in control.manifests
        )
    else:
        result = pipeline.parse_wave(
            str(uuid.uuid4()),
            page_kinds=["schedule"],
            settings=_settings(),
        )
        assert result.parsed == 1
        assert typed_writer.calls[0][0] == "schedule"
        assert control.manifests[-1]["dataset"] == "__page__"
        assert control.manifests[-1]["validation_status"] == "succeeded"
        typed_manifest = next(
            item for item in control.manifests
            if item["dataset"] == "typed:schedule"
        )
        assert typed_manifest["persistence_status"] == "succeeded"
        datasets = [item["dataset"] for item in control.manifests]
        assert datasets.index("typed:schedule") < datasets.index(
            "typed:__complete__"
        ) < datasets.index("__page__")


@pytest.mark.parametrize("typed_fails", [False, True])
def test_empty_typed_schedule_is_persisted_as_zero_row_replacement(
    tmp_path, typed_fails
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.registry["9"] = {
        "competition_id": "9",
        "canonical_url": "https://fbref.com/en/comps/9/history/x",
        "name": "Premier League",
        "gender": "male",
        "classification": "league:club",
        "metadata": {},
    }
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="schedule",
        canonical_url=(
            "https://fbref.com/en/comps/9/2025-2026/schedule/x"
        ),
        source_ids={"competition_id": "9", "season_id": "2025-2026"},
    ))
    refresh, record = _commit_for_parse(
        raw,
        target,
        '<table id="sched_all"><tbody></tbody></table>',
    )
    control.frontier[target.target_id] = {
        "target_id": target.target_id,
        "state": "fetched",
        "last_content_hash": record.content_hash,
    }
    control.fetches = [{
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "logical_refresh_id": refresh,
    }]
    typed_writer = FakeTypedWriter(fail=typed_fails)
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        typed_adapter=FakeTypedAdapter(typed_writer),
    )

    if typed_fails:
        with pytest.raises(ParseWaveError, match="typed persistence failed"):
            pipeline.parse_wave(
                str(uuid.uuid4()),
                page_kinds=["schedule"],
                settings=_settings(),
            )
    else:
        pipeline.parse_wave(
            str(uuid.uuid4()),
            page_kinds=["schedule"],
            settings=_settings(),
        )

    assert typed_writer.calls[0][0] == "schedule"
    assert typed_writer.calls[0][1].status.value == "empty"
    manifest = next(
        item
        for item in control.manifests
        if item["dataset"] == "typed:schedule"
    )
    assert manifest["row_count"] == 0
    assert manifest["persistence_status"] == (
        "failed" if typed_fails else "succeeded"
    )
    assert manifest["validation_status"] == (
        "failed" if typed_fails else "succeeded"
    )


@pytest.mark.unit
def test_initialize_run_reaps_leases_left_by_dead_workers(tmp_path):
    """A killed worker's fenced leases must not strand its targets forever:
    claim_targets only reaps the current run's leases, so the run start is the
    single place a global reap can happen."""

    class LeaseReapingControl(FakeControl):
        def __init__(self, raw_store):
            super().__init__(raw_store)
            self.reaped = 0

        def migrate(self):
            self.events.append("migrate")

        def reap_expired_leases(self):
            self.events.append("reap")
            self.reaped += 1
            return 3

        def create_run(self, run_type, **kwargs):
            self.events.append("create_run")

        def start_run(self, run_id):
            self.events.append("start_run")

    raw = _raw_store(tmp_path)
    control = LeaseReapingControl(raw)
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    run_id = pipeline.initialize_run(
        airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
        settings=_settings(),
    )

    assert control.reaped == 1
    assert control.events.index("reap") < control.events.index("create_run")
    assert uuid.UUID(run_id)


class FakeClearanceRejectedFetcher:
    """403s once (a dead cf_clearance), then serves the page normally."""

    def __init__(self, events):
        self.events = events
        self.calls = 0

    def __enter__(self):
        self.events.append("fetcher_enter")
        return self

    def __exit__(self, *args):
        self.events.append("fetcher_exit")

    def fetch(self, url, **kwargs):
        self.calls += 1
        self.events.append("http")
        raise FetchError(
            "FBref returned HTTP 403",
            error_class="http_status",
            http_status=403,
            wire_bytes=200,
            browser_document_bytes=500,
            browser_asset_bytes=100,
            browser_requests=1,
            browser_bootstrap_attempts=1,
            target_requests=1,
            http_status_history=(403,),
            latency_ms=100,
        )


def test_a_rejected_clearance_is_burned_instead_of_failing_every_target(tmp_path):
    """Cloudflare can stop honouring a clearance mid-wave (its exit IP falls out
    of favour). Reusing the dead session 403s every remaining target — one bad
    exit IP burned a whole production wave. The wave must re-solve on a fresh
    proxy instead."""
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    run_id = str(uuid.UUID(int=1))

    def lease(number, target_id, page_kind, canonical_url, source_ids):
        return TargetLease(
            attempt_id=str(uuid.UUID(int=30 + number)),
            run_id=run_id,
            target_id=target_id,
            logical_refresh_id=str(uuid.UUID(int=40 + number)),
            canonical_url=canonical_url,
            page_kind=page_kind,
            source_ids=source_ids,
            claim_token=str(uuid.UUID(int=50 + number)),
            lease_epoch=1,
            attempt_number=1,
            leased_by="worker-1",
            lease_expires_at=NOW + timedelta(minutes=10),
        )

    first = lease(
        1,
        "fbref:competition:9",
        "competition",
        "https://fbref.com/en/comps/9/history/Premier-League-Seasons",
        {"competition_id": "9"},
    )
    second = lease(
        2,
        "fbref:competition:12",
        "competition",
        "https://fbref.com/en/comps/12/history/La-Liga-Seasons",
        {"competition_id": "12"},
    )
    control.claim_targets = lambda *args, **kwargs: [first, second]
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda _: FakeClearanceRejectedFetcher(control.events),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    with pytest.raises(FetchWaveError):
        pipeline.fetch_wave(
            run_id,
            worker_id="worker-1",
            page_kinds=["competition"],
            settings=_settings(),
        )

    # The dead session is torn down and a fresh one solved for the next target,
    # rather than every target being fed to the same rejected clearance.
    assert control.events.count("session_open") == 2
    assert control.events.count("fetcher_exit") == 2


def test_a_run_that_hits_its_budget_requeues_its_targets_and_ends_clean(tmp_path):
    """The budget is a ceiling the crawler is meant to stop at. Failing the
    untouched targets made every day that spent its budget a red run — and left
    them backing off instead of ready for the next run."""
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    run_id = str(uuid.UUID(int=1))
    requeued = []

    def lease(number, target_id):
        return TargetLease(
            attempt_id=str(uuid.UUID(int=30 + number)),
            run_id=run_id,
            target_id=target_id,
            logical_refresh_id=str(uuid.UUID(int=40 + number)),
            canonical_url=f"https://fbref.com/en/comps/{number}/history/x-Seasons",
            page_kind="competition",
            source_ids={"competition_id": str(number)},
            claim_token=str(uuid.UUID(int=50 + number)),
            lease_epoch=1,
            attempt_number=1,
            leased_by="worker-1",
            lease_expires_at=NOW + timedelta(minutes=10),
        )

    leases = [lease(9, "fbref:competition:9"), lease(12, "fbref:competition:12")]
    control.claim_targets = lambda *args, **kwargs: leases

    def out_of_budget(*args, **kwargs):
        raise BudgetExceeded("request budget exhausted")

    control.reserve_budget = out_of_budget
    control.requeue_unfetched_targets = lambda items: (
        requeued.extend(items) or len(items)
    )
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda _: FakeFetcher(control.events, b"<html>ok</html>"),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        run_id,
        worker_id="worker-1",
        page_kinds=["competition"],
        settings=_settings(),
    )

    assert result.failures == []
    assert result.budget_exhausted is True
    assert result.requeued_at_budget == 2
    assert [item.target_id for item in requeued] == [
        "fbref:competition:9",
        "fbref:competition:12",
    ]
