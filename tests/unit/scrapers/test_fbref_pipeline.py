from __future__ import annotations

import hashlib
import gzip
import gc
import json
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scrapers.fbref.camoufox_fetch import GEOIP_BYTE_RESERVATION_BYTES
from scrapers.fbref.bronze import (
    GenericPagePersistItem,
    PAGE_MANIFEST_TABLE,
    TABLE_CELLS_TABLE,
    TABLE_INVENTORY_TABLE,
)
from scrapers.fbref.control import StateConflict
from scrapers.fbref.control.models import (
    BudgetReservation,
    CohortTarget,
    FrontierTarget,
    ObservationLease,
    TargetLease,
    ThrottleSlot,
)
from scrapers.fbref.control.store import BudgetExceeded
from scrapers.fbref.fetcher import (
    FETCHER_VERSION,
    PERSISTENT_SESSION_MAX_AGE_SECONDS,
    FetchError,
    FetchResponse,
    PersistentMeteredSessionReceipt,
)
from scrapers.fbref.match_parser import (
    DatasetParseResult,
    DatasetStatus,
    MatchParseResult,
)
from scrapers.fbref.page_document import PAGE_DOCUMENT_VERSION
from scrapers.fbref.pipeline import (
    FBrefPipeline,
    FETCH_LEASE_SECONDS,
    FetchWaveError,
    ParseWaveError,
    PipelineError,
    PipelineSettings,
    RunValidationError,
    SENTINEL_COMPETITIONS,
    WaveResult,
    affordable_clearance_reservation,
    backfill_season_cohort_capacity,
    frontier_target,
    live_wave_target_capacity,
    page_target_from_link,
    wave_target_capacity,
    _bounded_int,
    _LiveFetchSession,
    _session_failure,
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
from scrapers.fbref.settings import (
    DEFAULT_DOMAIN_INTERVAL_SECONDS,
    MIN_DOMAIN_INTERVAL_SECONDS,
)
from scrapers.fbref.typed_bronze import (
    MATCH_AVAILABILITY_TABLE,
    MATCH_DATASET_TABLES,
    TYPED_BRONZE_PARSER_VERSION,
    TypedMatchPersistItem,
    TypedSourceContext,
)


NOW = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)


def test_persistent_tail_reservation_failure_closes_empty_control_session():
    events = []

    class Fetcher:
        def begin_metered_session(self, session_id):
            events.append(("begin", session_id))
            return 0

        def finalize_metered_session(self):
            events.append("provider_finalized")
            return {"session_id": "session-1"}

    class Control:
        def reserve_clearance_session_tail(self, *_args, **_kwargs):
            raise RuntimeError("tail reserve unavailable")

        def close_clearance_session(self, session_id, *, status):
            events.append(("control_closed", session_id, status))

        def settle_clearance_session_tail(self, *_args, **_kwargs):
            raise AssertionError("a missing tail must not be settled")

    live = _LiveFetchSession(
        fetcher=Fetcher(), persistent_enabled=True
    )
    live.attach_control_session("session-1")

    with pytest.raises(RuntimeError, match="tail reserve unavailable"):
        live.begin_persistent(Control(), run_id="run-1", tail_bytes=10)
    live.finalize(Control(), status="failed")

    assert live.state == "control_closed"
    assert events == [
        ("begin", "session-1"),
        "provider_finalized",
        ("control_closed", "session-1", "failed"),
    ]


def test_terminal_persistent_tail_still_releases_closed_fetcher_stack():
    events = []

    class Fetcher:
        def finalize_metered_session(self):
            events.append("provider_finalized")
            return {"session_id": "session-1"}

    class Control:
        def settle_clearance_session_tail(self, _session_id, _receipt):
            events.append("tail_settled")
            return {"terminal": True}

        def close_clearance_session(self, _session_id, *, status):
            events.append(("control_closed", status))

    live = _LiveFetchSession(
        fetcher=Fetcher(), persistent_enabled=True
    )
    live.attach_control_session("session-1")
    live.state = "active"
    live.tail_reserved = True
    live.stack.callback(events.append, "stack_released")

    with pytest.raises(FetchWaveError, match="tail exceeded"):
        live.close(Control(), status="closed")

    assert live.fetcher is None
    assert live.session_id is None
    assert events == [
        "provider_finalized",
        "tail_settled",
        ("control_closed", "failed"),
        "stack_released",
    ]


def test_persistent_rollover_waits_for_retryable_tail_and_control_close():
    events = []

    class Fetcher:
        def begin_metered_session(self, session_id):
            events.append(("meter_begin", session_id))
            return 0

        def finalize_metered_session(self):
            events.append("provider_finalized")
            return {"session_id": "session-1"}

    class Control:
        settle_attempts = 0

        def reserve_clearance_session_tail(self, _run_id, session_id, **_kwargs):
            events.append(("tail_reserved", session_id))

        def settle_clearance_session_tail(self, session_id, _receipt):
            self.settle_attempts += 1
            events.append(("tail_settle", session_id, self.settle_attempts))
            if self.settle_attempts == 1:
                raise RuntimeError("temporary tail store failure")
            return {"terminal": False}

        def close_clearance_session(self, session_id, *, status):
            events.append(("control_closed", session_id, status))

    control = Control()
    live = _LiveFetchSession(fetcher=Fetcher(), persistent_enabled=True)
    live.attach_control_session("session-1")
    live.begin_persistent(control, run_id="run-1", tail_bytes=10)

    with pytest.raises(FetchWaveError, match="finalizer failed"):
        live.finalize(control, status="failed")
    with pytest.raises(PipelineError, match="not finalized"):
        live.attach_control_session("session-2")

    live.finalize(control, status="failed")
    live.attach_control_session("session-2")
    live.begin_persistent(control, run_id="run-1", tail_bytes=10)

    assert live.state == "active"
    assert events.index(("control_closed", "session-1", "failed")) < (
        events.index(("meter_begin", "session-2"))
    )


@pytest.mark.parametrize(
    ("request_limit", "byte_limit", "expected"),
    [
        (100, 50 * 1024 * 1024, 15),
        (200, 100 * 1024 * 1024, 25),
    ],
)
def test_wave_capacity_matches_live_canary_and_production_admission(
    request_limit, byte_limit, expected
):
    settings = PipelineSettings(
        run_type="backfill",
        request_limit=request_limit,
        byte_limit=byte_limit,
        shard_size=25,
    )

    assert wave_target_capacity(settings) == expected


@pytest.mark.parametrize(
    ("request_limit", "byte_limit", "expected"),
    [
        (100, 50 * 1024 * 1024, 7),
        (200, 100 * 1024 * 1024, 14),
    ],
)
def test_backfill_season_cohort_retains_conservative_aggregate_contract(
    request_limit, byte_limit, expected
):
    settings = PipelineSettings(
        run_type="backfill",
        request_limit=request_limit,
        byte_limit=byte_limit,
        shard_size=25,
    )

    assert backfill_season_cohort_capacity(settings) == expected


@pytest.mark.parametrize(
    ("request_limit", "byte_limit", "expected"),
    [
        (100, 50 * 1024 * 1024, 25),
        (200, 100 * 1024 * 1024, 25),
    ],
)
def test_live_wave_capacity_uses_sequential_byte_reservations(
    request_limit, byte_limit, expected
):
    settings = PipelineSettings(
        run_type="current",
        request_limit=request_limit,
        byte_limit=byte_limit,
        shard_size=25,
    )

    assert live_wave_target_capacity(settings) == expected
    assert live_wave_target_capacity(
        settings, request_remaining=19, byte_remaining=byte_limit
    ) == 0
    assert live_wave_target_capacity(
        settings, request_remaining=100, byte_remaining=6 * 1024 * 1024
    ) == 0


def test_persistent_capacity_funds_one_extra_zero_request_tail_reservation():
    common = dict(
        run_type="current",
        request_limit=100,
        byte_limit=1000,
        shard_size=25,
        request_reservation_bytes=100,
        bootstrap_request_reservation=1,
        bootstrap_byte_reservation=10,
    )
    legacy = PipelineSettings(**common, persistent_http_session=False)
    persistent = PipelineSettings(**common, persistent_http_session=True)

    assert wave_target_capacity(legacy) == 9
    assert wave_target_capacity(persistent) == 8
    assert live_wave_target_capacity(
        legacy, request_remaining=100, byte_remaining=110
    ) == 25
    assert live_wave_target_capacity(
        persistent, request_remaining=100, byte_remaining=110
    ) == 0
    assert affordable_clearance_reservation(
        legacy, request_remaining=3, byte_remaining=110
    ) == (1, 10)
    assert affordable_clearance_reservation(
        persistent, request_remaining=3, byte_remaining=110
    ) is None
    assert affordable_clearance_reservation(
        persistent, request_remaining=3, byte_remaining=210
    ) == (1, 10)


def test_acceptance_settings_are_fixed_live_and_zero_network_profiles():
    current = PipelineSettings.acceptance(scope="current")
    history = PipelineSettings.acceptance(scope="history")
    replay = PipelineSettings.acceptance_replay()

    assert (current.run_type, current.request_limit, current.byte_limit) == (
        "current",
        100,
        50 * 1024 * 1024,
    )
    assert current.shard_size == history.shard_size == 25
    assert history.run_type == "backfill"
    assert (replay.run_type, replay.request_limit, replay.byte_limit) == (
        "replay",
        0,
        0,
    )
    with pytest.raises(ValueError, match="current or history"):
        PipelineSettings.acceptance(scope="all")
    with pytest.raises(ValueError, match="exactly 25"):
        PipelineSettings.acceptance_replay(shard_size=24)


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
        self.batch_sizes = []
        self.batch_error = None

    def persist_page(self, page, **kwargs):
        self.pages.append((page, kwargs))
        return {"cells": 0, "tables": len(page.tables), "manifest": 1}

    def persist_pages(self, items):
        materialized = tuple(items)
        self.batch_sizes.append(len(materialized))
        if self.batch_error is not None:
            raise self.batch_error
        return [
            self.persist_page(
                item.page,
                canonical_url=item.canonical_url,
                run_id=item.run_id,
                staging_identity=item.staging_identity,
            )
            for item in materialized
        ]


class ContractWriter(FakeWriter):
    """Exercise the real writer's fail-closed handling of page.errors."""

    def persist_page(self, page, **kwargs):
        if page.errors:
            raise RuntimeError(f"generic contract failed: {page.errors}")
        return super().persist_page(page, **kwargs)


class FakeTypedWriter:
    def __init__(self, *, fail=False, events=None):
        self.fail = fail
        self.calls = []
        self.events = events
        self.batch_sizes = []
        self.batch_error = None

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
        if self.events is not None:
            self.events.append(f"typed_write:match:{kwargs['match_id']}")
        return {
            name: dataset.row_count
            for name, dataset in parsed.datasets.items()
            if dataset.status.value == "available"
        }

    def persist_matches(self, items):
        materialized = tuple(items)
        self.batch_sizes.append(len(materialized))
        if self.batch_error is not None:
            raise self.batch_error
        counts = [
            self.persist_match(
                item.parsed,
                match_id=item.match_id,
                context=item.context,
                run_id=item.run_id,
                target_identity=item.target_identity,
            )
            for item in materialized
        ]
        if self.events is not None:
            self.events.append("typed_batch:availability")
        return counts


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
        self.season_aliases = {}
        self.season_alias_calls = []
        self.manifests = []
        self.observations = {}
        self.provenance = []
        self.frontier_batches = []
        self.completed = []
        self.failed = []
        self.snapshots = []
        self.reservations = []
        self.settlements = []
        self.session_metrics = []
        self.heartbeats = []
        self.claim_calls = []
        self.observation_claim_calls = []
        self.eligible_competition_calls = 0
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

    def record_frontier_provenance(self, edge):
        record = {
            "parent_target_id": edge.parent_target_id,
            "child_target_id": edge.child_target_id,
            "relation": edge.relation,
            "carried_competition_id": edge.carried_competition_id,
            "carried_season_id": edge.carried_season_id,
            "parent_content_hash": edge.parent_content_hash,
            "parser_version": edge.parser_version,
            "logical_refresh_id": edge.logical_refresh_id,
            "metadata": dict(edge.metadata),
        }
        if record not in self.provenance:
            self.provenance.append(record)
        return f"edge-{len(self.provenance)}"

    def list_frontier_provenance(
        self, *, parent_target_id=None, child_target_id=None, limit=100
    ):
        rows = [
            row for row in self.provenance
            if (parent_target_id is None
                or row["parent_target_id"] == parent_target_id)
            and (child_target_id is None
                 or row["child_target_id"] == child_target_id)
        ]
        return rows[:limit]

    def reconcile_frontier_scope(self, *, source="fbref"):
        assert source == "fbref"
        self.events.append("scope_reconcile")
        return {"quarantined": 0, "reactivated": 0}

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

    def retry_session_fetch(self, lease, **kwargs):
        self.events.append("session_retry")
        self.failed.append((lease, {**kwargs, "session_retry": True}))

    def requeue_unfetched_targets(self, leases):
        items = list(leases)
        self.events.extend(f"requeue:{lease.target_id}" for lease in items)
        return len(items)

    @contextmanager
    def guard_publication_lock(self, run_id, *, source="fbref"):
        assert source == "fbref"
        yield {"owner_run_id": run_id, "active": True}

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

    def list_unprocessed_fetches(
        self,
        *,
        parser_version,
        typed_parser_version,
        stateful_parser_version,
        page_kinds,
        limit,
    ):
        rows = self.list_run_fetches(
            "all-runs",
            page_kinds=page_kinds,
            limit=limit,
            only_unparsed=True,
            parser_version=parser_version,
            typed_parser_version=typed_parser_version,
            stateful_parser_version=stateful_parser_version,
        )
        return [
            {
                "run_id": item.get("run_id", str(uuid.UUID(int=1))),
                "source_run_type": item.get(
                    "source_run_type", self.run["run_type"]
                ),
                **item,
            }
            for item in rows
            if self.frontier.get(
                str(item["target_id"]), {}
            ).get("state") != "quarantined"
        ]

    def quarantine_contract_rejected_target(
        self, target_id, *, content_hash, reason
    ):
        row = self.frontier.get(str(target_id))
        if row is None or row.get("state") in {"leased", "dead"}:
            return False
        if str(row.get("last_content_hash") or "") != str(content_hash):
            return False
        row.update(
            state="quarantined",
            next_fetch_at=None,
            retry_after=None,
            last_error_class="ParseContractQuarantined",
            last_error_message=str(reason),
        )
        self.events.append(f"contract_quarantine:{target_id}")
        return True

    def claim_observation_processing(self, **kwargs):
        self.observation_claim_calls.append(dict(kwargs))
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
        self.events.append(f"observation_fail:{lease.target_id}")

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
        self.events.append(f"manifest:{kwargs['target_id']}:{kwargs['dataset']}")

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
        self.eligible_competition_calls += 1
        return [
            row for row in self.registry.values() if row["gender"] == "male"
        ]

    def reconcile_seasons(self, snapshot_id, competition_id, entries):
        self.seasons.extend(entries)
        return {}

    def upsert_season_alias(self, alias, *, snapshot_id=None):
        key = (alias.source, alias.competition_id, alias.alias)
        previous = self.season_aliases.get(key)
        # A display label follows its season rollover; only an identity token
        # refuses to be remapped.  Mirrors ControlStore.upsert_season_alias.
        if (
            previous is not None
            and previous[0].season_id != alias.season_id
            and previous[0].alias_kind != "label"
        ):
            raise StateConflict(
                f"Season alias {alias.competition_id}/{alias.alias} "
                "is already mapped to a different season"
            )
        self.season_aliases[key] = (alias, snapshot_id)
        self.season_alias_calls.append((alias, snapshot_id))

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

    def upsert_frontier_discovery_batch(self, *, targets, provenance):
        targets = list(targets)
        provenance = list(provenance)
        self.frontier_batches.append((targets, provenance))
        self.events.append("frontier_batch:start")
        for target in targets:
            self.upsert_frontier_target(target)
        for edge in provenance:
            self.record_frontier_provenance(edge)
        self.events.append("frontier_batch:end")
        return {
            "target_count": len(targets),
            "provenance_count": len(provenance),
        }

    def create_run_cohort(self, run_id, cohort):
        self.events.append(f"explicit_cohort:{len(cohort)}")
        return len(cohort)

    def create_explicit_run_cohort(self, run_id, target_ids):
        cohort = [
            CohortTarget(
                target_id=str(target_id),
                logical_refresh_id=str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"fbref:{run_id}:{target_id}",
                    )
                ),
                ordinal=ordinal,
            )
            for ordinal, target_id in enumerate(target_ids)
        ]
        self.events.append(f"acceptance_cohort:{len(cohort)}")
        return cohort

    def record_acceptance_cohort(self, run_id, evidence):
        self.run.setdefault("metadata", {})["acceptance_cohort"] = dict(evidence)
        self.events.append("acceptance_cohort_anchored")
        return {**dict(evidence), "idempotent": False}

    def record_bronze_acceptance(self, run_id, evidence, *, replay=False):
        key = "bronze_acceptance_replay" if replay else "bronze_acceptance"
        self.run.setdefault("metadata", {})[key] = dict(evidence)
        self.events.append(f"{key}_anchored")
        return {**dict(evidence), "idempotent": False}

    def get_acceptance_run_evidence(self, run_id):
        return getattr(self, "acceptance_evidence", None)

    def get_run_summary(self, run_id, **_versions):
        return {
            **self.run,
            "budget_exceeded": False,
            "target_counts": {"succeeded": 1},
            "dataset_validation_counts": {"succeeded": 1},
            "sentinel_coverage": _complete_sentinel_coverage(),
            "unknown_gender_registry_count": 0,
            "unvalidated_target_count": 0,
            "unprocessed_raw_count": 0,
            "global_unprocessed_raw_count": 0,
            "global_unprocessed_raw_sla_overdue_count": 0,
            "crawlable_frontier_scope_counts": {"eligible_male": 1},
            "current_scope_freshness": {
                "total_targets": 1,
                "fresh_targets": 1,
                "stale_targets": 0,
                "all_within_sla": True,
            },
        }

    def finish_run(self, run_id, *, succeeded):
        self.events.append(f"finish:{succeeded}")


class BudgetAwareFakeControl(FakeControl):
    """Small in-memory copy of the control store's atomic budget ceiling."""

    def __init__(self, raw_store=None):
        super().__init__(raw_store)
        self._open_budget_reservations = {}

    def reserve_budget(self, *args, **kwargs):
        requests = int(kwargs["requests"])
        bytes_ = int(kwargs["bytes_"])
        if (
            self.run["requests_used"]
            + self.run["requests_reserved"]
            + requests
            > self.run["request_limit"]
            or self.run["bytes_used"]
            + self.run["bytes_reserved"]
            + bytes_
            > self.run["byte_limit"]
        ):
            raise BudgetExceeded("fake run budget exhausted")
        reservation_id = str(
            uuid.UUID(int=500 + len(self.reservations))
        )
        self.events.append("reserve")
        self.reservations.append((args, kwargs))
        self.run["requests_reserved"] += requests
        self.run["bytes_reserved"] += bytes_
        self._open_budget_reservations[reservation_id] = (requests, bytes_)
        return BudgetReservation(
            reservation_id=reservation_id,
            run_id=args[0],
            logical_refresh_id=args[1],
            requests_reserved=requests,
            bytes_reserved=bytes_,
            status="reserved",
        )

    def settle_budget(self, reservation_id, **kwargs):
        requests, bytes_ = self._open_budget_reservations.pop(reservation_id)
        self.run["requests_reserved"] -= requests
        self.run["bytes_reserved"] -= bytes_
        super().settle_budget(reservation_id, **kwargs)


class PersistentFakeControl(BudgetAwareFakeControl):
    def __init__(self, raw_store=None):
        super().__init__(raw_store)
        self.run["metadata"] = {"persistent_http_session": True}
        self.page_evidence = []
        self.tail_evidence = []
        self._tail_reserved = 0
        self.force_page_budget_exceeded = False
        self.force_tail_over_reservation = False

    def reserve_clearance_session_tail(
        self,
        run_id,
        session_id,
        *,
        bytes_reserved,
        baseline_provider_bytes,
    ):
        self.events.append("tail_reserve")
        self._tail_reserved = int(bytes_reserved)
        self.run["bytes_reserved"] += self._tail_reserved
        return {
            "run_id": run_id,
            "session_id": session_id,
            "bytes_reserved": bytes_reserved,
            "baseline_provider_bytes": baseline_provider_bytes,
        }

    def settle_clearance_session_page(
        self, session_id, reservation_id, **kwargs
    ):
        self.events.append("page_settle")
        requests_reserved, bytes_reserved = (
            self._open_budget_reservations.pop(reservation_id)
        )
        self.run["requests_reserved"] -= requests_reserved
        self.run["bytes_reserved"] -= bytes_reserved
        self.run["requests_used"] += int(kwargs["requests_used"])
        self.run["bytes_used"] += int(kwargs["provider_billed_bytes"])
        self.page_evidence.append((session_id, reservation_id, dict(kwargs)))
        return {
            "budget_exceeded": self.force_page_budget_exceeded,
            "idempotent": False,
        }

    def settle_clearance_session_tail(self, session_id, receipt):
        self.events.append("tail_settle")
        self.run["bytes_reserved"] -= self._tail_reserved
        self.run["bytes_used"] += int(receipt.tail_provider_bytes)
        self._tail_reserved = 0
        self.tail_evidence.append((session_id, receipt))
        return {
            "terminal": (
                self.force_page_budget_exceeded
                or self.force_tail_over_reservation
            ),
            "budget_exceeded_by_tail": self.force_tail_over_reservation,
            "tail_over_reservation": self.force_tail_over_reservation,
            "idempotent": False,
        }

    def assert_persistent_metering_reconciled(self, run_id):
        assert self.run["requests_reserved"] == 0
        assert self.run["bytes_reserved"] == 0
        assert self.run["bytes_used"] == 130
        self.events.append("persistent_reconcile")
        return {"run_id": run_id, "reconciled": True}


class PersistentFakeFetcher:
    persistent_http_session = True

    def __init__(self, events):
        self.events = events
        self.session_id = None
        self.receipt = None

    def __enter__(self):
        self.events.append("fetcher_enter")
        return self

    def __exit__(self, *_args):
        self.events.append("fetcher_exit")

    def begin_metered_session(self, session_id):
        self.session_id = session_id
        self.receipt = None
        self.events.append("meter_begin")
        return 0

    def ensure_clearance(self):
        self.events.append("browser")
        return True

    def persistent_session_rollover_due(self, *, within_seconds=0):
        assert within_seconds >= 0
        return False

    def fetch(self, url, **_kwargs):
        self.events.append("http")
        return FetchResponse(
            url=url,
            status_code=200,
            body=b"<html><table></table></html>",
            headers={"content-type": "text/html"},
            latency_ms=5,
            http_wire_bytes=80,
            decoded_html_bytes=28,
            http_requests=1,
            http_status_history=(200,),
            browser_document_bytes=20,
            browser_asset_bytes=10,
            browser_requests=1,
            browser_bootstrap_attempts=1,
            provider_billed_bytes=120,
        )

    def finalize_metered_session(self):
        if self.receipt is None:
            self.events.append("provider_finalize")
            self.receipt = PersistentMeteredSessionReceipt(
                session_id=self.session_id,
                meter="proxy_filter_provider_path_v2",
                baseline_provider_bytes=0,
                page_provider_bytes=120,
                authoritative_provider_bytes=130,
                tail_provider_bytes=10,
            )
        return self.receipt


class FakeFetcher:
    def __init__(self, events, body, *, http_requests=1):
        self.events = events
        self.body = body
        self.http_requests = http_requests
        self.clearance_ready = False

    def __enter__(self):
        self.events.append("fetcher_enter")
        return self

    def __exit__(self, *args):
        self.events.append("fetcher_exit")

    def ensure_clearance(self):
        if self.clearance_ready:
            return False
        self.clearance_ready = True
        self.events.append("browser")
        return True

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

    def ensure_clearance(self):
        self.events.append("browser")
        return True

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
        domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
    )


def _persistent_settings():
    return replace(_settings(), persistent_http_session=True)


def test_persistent_fetch_commits_raw_before_page_tail_and_control_close(
    tmp_path, monkeypatch
):
    raw = _raw_store(tmp_path)
    control = PersistentFakeControl(raw)
    fetcher = PersistentFakeFetcher(control.events)
    original_commit = raw.commit_fetch

    def commit_with_event(*args, **kwargs):
        control.events.append("raw_commit")
        return original_commit(*args, **kwargs)

    monkeypatch.setattr(raw, "commit_fetch", commit_with_event)
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda *_args: fetcher,
    )

    result = pipeline.fetch_wave(
        str(uuid.UUID(int=1)),
        worker_id="persistent-worker",
        page_kinds=["competition_index"],
        settings=_persistent_settings(),
    )

    assert result.fetched == 1
    evidence = control.page_evidence[0][2]
    assert evidence["provider_billed_bytes"] == 120
    assert evidence["compressed_raw_bytes"] > 0
    expected_order = (
        "raw_commit",
        "page_settle",
        "provider_finalize",
        "tail_settle",
        "session_close",
        "fetcher_exit",
        "persistent_reconcile",
    )
    positions = [control.events.index(item) for item in expected_order]
    assert positions == sorted(positions)


def test_persistent_raw_store_failure_settles_authoritative_page_once(
    tmp_path, monkeypatch
):
    raw = _raw_store(tmp_path)
    control = PersistentFakeControl(raw)
    fetcher = PersistentFakeFetcher(control.events)
    monkeypatch.setattr(
        raw,
        "commit_fetch",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            OSError("raw store unavailable")
        ),
    )
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda *_args: fetcher,
    )

    with pytest.raises(FetchWaveError, match="OSError"):
        pipeline.fetch_wave(
            str(uuid.UUID(int=1)),
            worker_id="persistent-worker",
            page_kinds=["competition_index"],
            settings=_persistent_settings(),
        )

    assert len(control.page_evidence) == 1
    evidence = control.page_evidence[0][2]
    assert evidence["provider_billed_bytes"] == 120
    assert evidence["compressed_raw_bytes"] == 0
    assert control.events.index("page_settle") < control.events.index("fail")
    assert control.events.index("fail") < control.events.index(
        "provider_finalize"
    )
    assert len(control.tail_evidence) == 1


def _persistent_budget_leases(run_id):
    return [
        TargetLease(
            attempt_id=str(uuid.UUID(int=610 + number)),
            run_id=run_id,
            target_id=f"fbref:competition:{number}",
            logical_refresh_id=str(uuid.UUID(int=620 + number)),
            canonical_url=(
                f"https://fbref.com/en/comps/{number}/history/x-Seasons"
            ),
            page_kind="competition",
            source_ids={"competition_id": str(number)},
            claim_token=str(uuid.UUID(int=630 + number)),
            lease_epoch=1,
            attempt_number=1,
            leased_by="persistent-budget",
            lease_expires_at=NOW + timedelta(minutes=10),
        )
        for number in (9, 12)
    ]


def test_persistent_canary_page_overrun_completes_success_and_requeues_rest(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = PersistentFakeControl(raw)
    control.force_page_budget_exceeded = True
    run_id = str(uuid.UUID(int=1))
    leases = _persistent_budget_leases(run_id)
    control.claim_targets = lambda *_args, **_kwargs: leases
    fetcher = PersistentFakeFetcher(control.events)
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda *_args: fetcher,
        sleep=lambda _seconds: None,
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        run_id,
        worker_id="persistent-budget",
        page_kinds=["competition"],
        settings=_persistent_settings(),
    )

    assert result.fetched == 1
    assert result.budget_exhausted is True
    assert result.requeued_at_budget == 1
    assert result.failures == []
    assert [item[0].target_id for item in control.completed] == [
        leases[0].target_id
    ]
    assert f"requeue:{leases[1].target_id}" in control.events
    assert len(control.tail_evidence) == 1


def test_persistent_canary_fetch_error_overrun_requeues_current_and_rest(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = PersistentFakeControl(raw)
    control.force_page_budget_exceeded = True
    run_id = str(uuid.UUID(int=1))
    leases = _persistent_budget_leases(run_id)
    control.claim_targets = lambda *_args, **_kwargs: leases

    class ErrorFetcher(PersistentFakeFetcher):
        def fetch(self, url, **_kwargs):
            self.events.append("http_error")
            raise FetchError(
                "clearance rejected at the canary boundary",
                error_class="http_status",
                http_status=403,
                wire_bytes=80,
                browser_document_bytes=20,
                browser_asset_bytes=10,
                browser_requests=1,
                browser_bootstrap_attempts=1,
                provider_billed_bytes=120,
                target_requests=1,
                http_status_history=(403,),
            )

    fetcher = ErrorFetcher(control.events)
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda *_args: fetcher,
        sleep=lambda _seconds: None,
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        run_id,
        worker_id="persistent-budget",
        page_kinds=["competition"],
        settings=_persistent_settings(),
    )

    assert result.budget_exhausted is True
    assert result.requeued_at_budget == 2
    assert result.failures == []
    assert control.failed[0][1]["requeue"] is True
    assert control.failed[0][1]["provider_billed_bytes"] == 120
    assert f"requeue:{leases[1].target_id}" in control.events
    assert len(control.tail_evidence) == 1


def test_persistent_production_page_overrun_is_loud_after_exact_close(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = PersistentFakeControl(raw)
    settings = replace(
        PipelineSettings(), persistent_http_session=True, shard_size=2
    )
    control.run.update(
        request_limit=settings.request_limit,
        byte_limit=settings.byte_limit,
    )
    control.force_page_budget_exceeded = True
    run_id = str(uuid.UUID(int=1))
    leases = _persistent_budget_leases(run_id)
    control.claim_targets = lambda *_args, **_kwargs: leases
    fetcher = PersistentFakeFetcher(control.events)
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda *_args: fetcher,
        sleep=lambda _seconds: None,
        clock=lambda: NOW,
    )

    with pytest.raises(
        FetchWaveError, match="production_safety_circuit_exhausted"
    ):
        pipeline.fetch_wave(
            run_id,
            worker_id="persistent-budget",
            page_kinds=["competition"],
            settings=settings,
        )

    assert f"requeue:{leases[1].target_id}" in control.events
    assert control.events.index("tail_settle") < control.events.index(
        "session_close"
    )


def test_page_budget_latch_does_not_hide_true_tail_over_reservation():
    events = []

    class Fetcher:
        def finalize_metered_session(self):
            events.append("provider_finalized")
            return {"session_id": "session-1"}

    class Control:
        def settle_clearance_session_tail(self, _session_id, _receipt):
            events.append("tail_settled")
            return {
                "terminal": True,
                "budget_exceeded_by_tail": True,
                "tail_over_reservation": True,
            }

        def close_clearance_session(self, _session_id, *, status):
            events.append(("control_closed", status))

    live = _LiveFetchSession(fetcher=Fetcher(), persistent_enabled=True)
    live.attach_control_session("session-1")
    live.state = "active"
    live.tail_reserved = True
    live.page_budget_latched = True

    with pytest.raises(FetchWaveError, match="tail exceeded"):
        live.close(Control(), status="closed")

    assert events == [
        "provider_finalized",
        "tail_settled",
        ("control_closed", "failed"),
    ]


def test_settings_cannot_underreserve_bounded_status_retry_requests():
    with pytest.raises(ValueError, match="cover both HTTP attempts"):
        PipelineSettings(target_request_reservation=1)


def test_settings_enforce_the_published_fbref_page_rate():
    assert PipelineSettings().domain_interval_seconds == (
        DEFAULT_DOMAIN_INTERVAL_SECONDS
    )
    with pytest.raises(ValueError, match="source minimum"):
        PipelineSettings(
            domain_interval_seconds=MIN_DOMAIN_INTERVAL_SECONDS - 0.001
        )


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


MATCH_FIXTURE = (
    Path(__file__).parents[2]
    / "fixtures"
    / "fbref"
    / "matches"
    / "0701e218.html.gz"
)


def _pipeline_with_saved_matches(tmp_path, match_ids=("0701e218", "a071faa8")):
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
    html = gzip.decompress(MATCH_FIXTURE.read_bytes()).decode("utf-8")
    records = []
    for position, match_id in enumerate(match_ids):
        target_id = f"fbref:match:{match_id}:{position}"
        target = PageTarget(
            source="fbref",
            page_kind="match",
            target_id=target_id,
            canonical_url=(
                f"https://fbref.com/en/matches/{match_id}/x-{position}"
            ),
            source_ids={
                "competition_id": "9",
                "season_id": "2025-2026",
                "match_id": match_id,
            },
        )
        refresh, record = _commit_for_parse(raw, target, html)
        records.append(record)
        control.frontier[target.target_id] = {
            "target_id": target.target_id,
            "page_kind": "match",
            "state": "fetched",
            "last_content_hash": record.content_hash,
            "last_logical_refresh_id": refresh,
        }
        control.fetches.append({
            "target_id": target.target_id,
            "page_kind": "match",
            "logical_refresh_id": refresh,
            "content_hash": record.content_hash,
        })
    generic = FakeWriter()
    typed = FakeTypedWriter(events=control.events)

    def forbidden_transport(*_args):
        raise AssertionError("parse batching must not construct a transport")

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=generic,
        typed_adapter=FakeTypedAdapter(typed),
        fetcher_factory=forbidden_transport,
    )
    pipeline.batch_persist_enabled = True
    return pipeline, control, records, generic, typed


GENERIC_DURABLE_STAGES = (
    TABLE_CELLS_TABLE,
    TABLE_INVENTORY_TABLE,
    PAGE_MANIFEST_TABLE,
)
TYPED_DURABLE_STAGES = (
    *MATCH_DATASET_TABLES.values(),
    MATCH_AVAILABILITY_TABLE,
)


def _stable(value):
    if isinstance(value, dict):
        return tuple(
            (str(key), _stable(item))
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        )
    if isinstance(value, (list, tuple)):
        return tuple(_stable(item) for item in value)
    return value


def _digest(value):
    rendered = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


class DurableGenericWriter:
    """Natural-keyed generic tables with one post-commit batch fault."""

    def __init__(self, *, fault_after_stage=None):
        self.fault_after_stage = fault_after_stage
        self.faulted = False
        self.fault_was_durable = False
        self.state = {stage: {} for stage in GENERIC_DURABLE_STAGES}
        self.batch_page_ids = []
        self.singleton_page_ids = []

    @staticmethod
    def _counts(page):
        return {
            "cells": len(page.cell_records()),
            "tables": len(page.tables),
            "manifest": 1,
        }

    def _commit(self, stage, item):
        page = item.page
        if stage == TABLE_CELLS_TABLE:
            for record in page.cell_records():
                key = (
                    record["table_instance_id"],
                    record["row_id"],
                    record["cell_id"],
                    record["parser_version"],
                )
                self.state[stage][key] = _digest(record)
            return
        if stage == TABLE_INVENTORY_TABLE:
            for record in page.inventory_records():
                key = (
                    record["table_instance_id"],
                    record["parser_version"],
                )
                self.state[stage][key] = _digest(record)
            return
        key = (page.target_id, page.content_hash, page.parser_version)
        self.state[stage][key] = _digest({
            "target_id": page.target_id,
            "canonical_url": item.canonical_url,
            "page_kind": page.page_kind,
            "content_hash": page.content_hash,
            "parser_version": page.parser_version,
            "table_count": len(page.tables),
            "cell_count": len(page.cell_records()),
            "errors": page.errors,
        })

    def persist_page(
        self,
        page,
        *,
        canonical_url,
        run_id,
        staging_identity,
    ):
        item = GenericPagePersistItem(
            page=page,
            canonical_url=canonical_url,
            run_id=run_id,
            staging_identity=staging_identity,
        )
        self.singleton_page_ids.append(id(page))
        for stage in GENERIC_DURABLE_STAGES:
            self._commit(stage, item)
        return self._counts(page)

    def persist_pages(self, items):
        materialized = tuple(items)
        self.batch_page_ids.extend(id(item.page) for item in materialized)
        for stage in GENERIC_DURABLE_STAGES:
            for item in materialized:
                self._commit(stage, item)
            if not self.faulted and stage == self.fault_after_stage:
                self.faulted = True
                self.fault_was_durable = bool(self.state[stage])
                raise RuntimeError(f"generic durable fault after {stage}")
        return [self._counts(item.page) for item in materialized]

    def snapshot(self):
        return _stable(self.state)


class DurableTypedWriter:
    """Match/availability tables with one post-commit batch fault."""

    def __init__(self, *, fault_after_stage=None, misalign=False):
        self.fault_after_stage = fault_after_stage
        self.misalign = misalign
        self.faulted = False
        self.fault_was_durable = False
        self.state = {stage: {} for stage in TYPED_DURABLE_STAGES}
        self.batch_parsed_ids = []
        self.singleton_parsed_ids = []
        self.batch_sizes = []

    @staticmethod
    def _dataset_value(result):
        status = str(getattr(result.status, "value", result.status))
        frame = result.frame
        return (
            status,
            result.row_count,
            result.reason,
            result.error_type,
            None
            if frame is None
            else hashlib.sha256(
                frame.to_json(
                    orient="split", date_format="iso", default_handler=str
                ).encode("utf-8")
            ).hexdigest(),
        )

    @staticmethod
    def _counts(item):
        return {
            dataset: result.row_count
            for dataset, result in item.parsed.datasets.items()
        }

    def _commit(self, stage, item):
        if stage == MATCH_AVAILABILITY_TABLE:
            for dataset, result in item.parsed.datasets.items():
                self.state[stage][(item.match_id, dataset)] = (
                    str(getattr(result.status, "value", result.status)),
                    result.row_count,
                    result.reason,
                    result.error_type,
                )
            return
        dataset = next(
            name for name, table in MATCH_DATASET_TABLES.items()
            if table == stage
        )
        self.state[stage][item.match_id] = self._dataset_value(
            item.parsed.datasets[dataset]
        )

    def persist_match(
        self,
        parsed,
        *,
        match_id,
        context,
        run_id,
        target_identity,
    ):
        item = TypedMatchPersistItem(
            parsed=parsed,
            match_id=match_id,
            context=context,
            run_id=run_id,
            target_identity=target_identity,
        )
        self.singleton_parsed_ids.append(id(parsed))
        for stage in TYPED_DURABLE_STAGES:
            self._commit(stage, item)
        return self._counts(item)

    def persist_matches(self, items):
        materialized = tuple(items)
        self.batch_sizes.append(len(materialized))
        self.batch_parsed_ids.extend(id(item.parsed) for item in materialized)
        for stage in TYPED_DURABLE_STAGES:
            for item in materialized:
                self._commit(stage, item)
            if not self.faulted and stage == self.fault_after_stage:
                self.faulted = True
                self.fault_was_durable = bool(self.state[stage])
                raise RuntimeError(f"typed durable fault after {stage}")
        counts = [self._counts(item) for item in materialized]
        return counts[:-1] if self.misalign else counts

    def snapshot(self):
        return _stable(self.state)


def _normalized_control_state(control):
    manifests = {}
    for item in control.manifests:
        key = (
            item["target_id"],
            item["content_hash"],
            item["parser_version"],
            item["dataset"],
        )
        manifests[key] = _stable({
            name: item.get(name)
            for name in (
                "availability",
                "parse_status",
                "persistence_status",
                "validation_status",
                "row_count",
                "error_class",
                "error_message",
            )
        })
    observations = {
        row["target_id"]: _stable({
            name: row.get(name)
            for name in ("status", "typed_status", "stateful_status")
        })
        for row in control.observations.values()
    }
    frontier = {
        target_id: _stable({
            name: row.get(name)
            for name in (
                "page_kind",
                "source_ids",
                "refresh_policy",
                "state",
                "next_fetch_at",
            )
        })
        for target_id, row in control.frontier.items()
    }
    provenance = sorted(
        _stable({
            name: item.get(name)
            for name in (
                "parent_target_id",
                "child_target_id",
                "relation",
                "carried_competition_id",
                "carried_season_id",
                "parent_content_hash",
                "parser_version",
                "metadata",
            )
        })
        for item in control.provenance
    )
    return _stable({
        "manifests": manifests,
        "observations": observations,
        "frontier": frontier,
        "provenance": provenance,
    })


@dataclass(frozen=True)
class DurableReplayCase:
    snapshot: object
    raw_loads: int
    observation_claims: int
    network_calls: int
    same_generic_objects: bool
    same_typed_objects: bool
    fault_was_durable: bool
    guard_entries: int


class DurableReplayPipeline(FBrefPipeline):
    """Keep the fault matrix focused on persistence, not match parsing."""

    @staticmethod
    def _parse_typed_match(_html, record):
        context = TypedSourceContext("9", "2025-2026")
        parsed = MatchParseResult(
            parser_version=TYPED_BRONZE_PARSER_VERSION,
            parsed_at="2026-08-08T00:00:00+00:00",
            status=DatasetStatus.AVAILABLE,
            datasets={
                name: DatasetParseResult(name, DatasetStatus.EMPTY)
                for name in MATCH_DATASET_TABLES
            },
        )
        return parsed, str(record.source_ids["match_id"]), context

    def _persist_typed(self, run_id, html, record):
        parsed, match_id, context = self._parse_typed_match(html, record)
        self._persist_preparsed_typed_match(
            run_id,
            record,
            parsed,
            match_id=match_id,
            context=context,
        )


def _pipeline_with_small_durable_matches(path):
    raw = _raw_store(path)
    control = FakeControl(raw)
    control.registry["9"] = {
        "competition_id": "9",
        "canonical_url": "https://fbref.com/en/comps/9/history/x",
        "name": "Premier League",
        "gender": "male",
        "classification": "league:club",
        "metadata": {},
    }
    records = []
    html = """
    <html><body><table id="match_summary">
      <thead><tr><th data-stat="label">Label</th></tr></thead>
      <tbody><tr><td data-stat="label">durable</td></tr></tbody>
    </table></body></html>
    """
    for position, match_id in enumerate(("durable-a", "durable-b")):
        target = PageTarget(
            source="fbref",
            page_kind="match",
            target_id=f"fbref:match:{match_id}",
            canonical_url=(
                f"https://fbref.com/en/matches/{match_id}/x-{position}"
            ),
            source_ids={
                "competition_id": "9",
                "season_id": "2025-2026",
                "match_id": match_id,
            },
        )
        refresh, record = _commit_for_parse(raw, target, html)
        records.append(record)
        control.frontier[target.target_id] = {
            "target_id": target.target_id,
            "page_kind": "match",
            "state": "fetched",
            "last_content_hash": record.content_hash,
            "last_logical_refresh_id": refresh,
        }
        control.fetches.append({
            "target_id": target.target_id,
            "page_kind": "match",
            "logical_refresh_id": refresh,
            "content_hash": record.content_hash,
        })
    pipeline = DurableReplayPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        typed_adapter=FakeTypedAdapter(FakeTypedWriter()),
        fetcher_factory=lambda *_args: (_ for _ in ()).throw(
            AssertionError("durable replay cannot fetch")
        ),
    )

    pipeline.batch_persist_enabled = True
    return pipeline, control, records


def _durable_replay_case(
    path,
    *,
    batch,
    generic_fault_stage=None,
    typed_fault_stage=None,
    typed_misalign=False,
):
    pipeline, control, _records = _pipeline_with_small_durable_matches(path)
    generic = DurableGenericWriter(fault_after_stage=generic_fault_stage)
    typed = DurableTypedWriter(
        fault_after_stage=typed_fault_stage,
        misalign=typed_misalign,
    )
    pipeline.generic_writer = generic
    pipeline.typed_adapter = FakeTypedAdapter(typed)
    pipeline.batch_persist_enabled = batch
    raw_loads = 0
    original_load = pipeline.raw_store.load_fetch_html

    def counted_load(logical_refresh_id):
        nonlocal raw_loads
        raw_loads += 1
        return original_load(logical_refresh_id)

    pipeline.raw_store.load_fetch_html = counted_load
    network_calls = 0

    def forbidden_transport(*_args):
        nonlocal network_calls
        network_calls += 1
        raise AssertionError("durable replay must not construct a transport")

    pipeline.fetcher_factory = forbidden_transport
    result = pipeline.parse_wave(
        str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
    )
    assert result.parsed == 2
    faulted_writer = generic if generic_fault_stage else typed
    case = DurableReplayCase(
        snapshot=_stable({
            "generic": generic.snapshot(),
            "typed": typed.snapshot(),
            "control": _normalized_control_state(control),
        }),
        raw_loads=raw_loads,
        observation_claims=len(control.observation_claim_calls),
        network_calls=network_calls,
        same_generic_objects=(
            generic_fault_stage is None
            or set(generic.batch_page_ids) == set(generic.singleton_page_ids)
        ),
        same_typed_objects=(
            typed_fault_stage is None and not typed_misalign
            or set(typed.batch_parsed_ids) == set(typed.singleton_parsed_ids)
        ),
        fault_was_durable=(
            False if not batch else faulted_writer.fault_was_durable
        ),
        guard_entries=sum(
            event.startswith("content_guard:") for event in control.events
        ),
    )
    # BeautifulSoup and parser objects contain cycles. The fault matrix runs
    # many complete saved-HTML replays in one pytest process, so collect those
    # bounded per-case graphs instead of retaining them until cyclic GC fires.
    del pipeline, control, generic, typed
    gc.collect()
    return case


@pytest.fixture(scope="module")
def durable_sequential_baseline(tmp_path_factory):
    return _durable_replay_case(
        tmp_path_factory.mktemp("fbref-durable-baseline"), batch=False
    )


def test_batch_persist_configuration_is_bounded_and_default_off(monkeypatch):
    pipeline = FBrefPipeline(
        FakeControl(), object(), generic_writer=FakeWriter()
    )
    assert pipeline.batch_persist_enabled is False
    assert pipeline.batch_persist_matches == 8
    assert pipeline.batch_persist_max_cells == 150000
    monkeypatch.delenv("FBREF_BATCH_PERSIST_MATCHES", raising=False)
    assert _bounded_int(
        "FBREF_BATCH_PERSIST_MATCHES", default=8, lower=2, upper=25
    ) == 8
    monkeypatch.setenv("FBREF_BATCH_PERSIST_MATCHES", "2")
    assert _bounded_int(
        "FBREF_BATCH_PERSIST_MATCHES", default=8, lower=2, upper=25
    ) == 2
    monkeypatch.setenv("FBREF_BATCH_PERSIST_MATCHES", "26")
    with pytest.raises(ValueError, match="between 2 and 25"):
        _bounded_int(
            "FBREF_BATCH_PERSIST_MATCHES", default=8, lower=2, upper=25
        )


def test_match_parse_wave_batches_writers_and_completes_each_lease(tmp_path):
    pipeline, control, records, generic, typed = (
        _pipeline_with_saved_matches(tmp_path)
    )

    result = pipeline.parse_wave(
        str(uuid.uuid4()),
        page_kinds=["match"],
        settings=_settings(),
    )

    assert generic.batch_sizes == [2]
    assert typed.batch_sizes == [2]
    assert result.claimed == result.parsed == result.typed_promoted == 2
    assert all(row["status"] == "succeeded" for row in control.observations.values())
    for record in records:
        availability = control.events.index("typed_batch:availability")
        typed_complete = control.events.index(
            f"manifest:{record.target_id}:typed:__complete__"
        )
        page_complete = control.events.index(
            f"manifest:{record.target_id}:__page__"
        )
        observation_complete = control.events.index(
            f"observation_complete:{record.target_id}"
        )
        assert availability < typed_complete < page_complete < observation_complete


@pytest.mark.parametrize("writer_name", ["generic", "typed"])
def test_match_batch_failure_reuses_claims_and_raw_without_network(
    tmp_path, writer_name
):
    pipeline, control, _records, generic, typed = (
        _pipeline_with_saved_matches(tmp_path)
    )
    writer = generic if writer_name == "generic" else typed
    writer.batch_error = RuntimeError("commit reset")
    load_calls = 0
    original_load = pipeline.raw_store.load_fetch_html

    def counted_load(logical_refresh_id):
        nonlocal load_calls
        load_calls += 1
        return original_load(logical_refresh_id)

    pipeline.raw_store.load_fetch_html = counted_load

    result = pipeline.parse_wave(
        str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
    )

    assert result.claimed == result.parsed == 2
    assert load_calls == 2
    assert len(control.observation_claim_calls) == 2
    assert len(control.observations) == 2
    assert all(row["status"] == "succeeded" for row in control.observations.values())
    if writer_name == "generic":
        assert len(generic.pages) == 2
    else:
        assert len(generic.pages) == 2
        assert [call[0] for call in typed.calls] == ["match", "match"]
        guards = [
            event for event in control.events
            if event.startswith("content_guard:")
        ]
        assert len(guards) == 2
        for call in typed.calls:
            target_identity = call[2]["target_identity"]
            record = next(
                record for record in _records
                if record.logical_refresh_id == target_identity
            )
            write = control.events.index(
                f"typed_write:match:{call[2]['match_id']}"
            )
            guard_exit = control.events.index(
                f"content_guard_exit:{record.target_id}"
            )
            assert write < guard_exit


def test_duplicate_match_identity_uses_claimed_sequential_fallback(tmp_path):
    pipeline, control, records, generic, typed = _pipeline_with_saved_matches(
        tmp_path
    )
    original_load = pipeline.raw_store.load_fetch_html

    def duplicate_match_id(logical_refresh_id):
        html, record = original_load(logical_refresh_id)
        if record.logical_refresh_id == records[1].logical_refresh_id:
            record = replace(
                record,
                source_ids={**record.source_ids, "match_id": "0701e218"},
            )
        return html, record

    pipeline.raw_store.load_fetch_html = duplicate_match_id
    # The fake control can represent duplicate source rows even though the raw
    # store target is the same. Each observation remains independently leased.
    result = pipeline.parse_wave(
        str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
    )

    assert result.claimed == result.parsed == 2
    assert generic.batch_sizes == []
    assert typed.batch_sizes == []
    assert len(generic.pages) == len(typed.calls) == 2
    assert all(row["status"] == "succeeded" for row in control.observations.values())


def test_duplicate_target_identity_uses_claimed_sequential_fallback(tmp_path):
    pipeline, control, records, generic, typed = _pipeline_with_saved_matches(
        tmp_path
    )
    original_load = pipeline.raw_store.load_fetch_html
    duplicate_target = records[0].target_id
    control.fetches[1]["target_id"] = duplicate_target

    def load_with_duplicate_target(logical_refresh_id):
        html, record = original_load(logical_refresh_id)
        if record.logical_refresh_id == records[1].logical_refresh_id:
            record = replace(record, target_id=duplicate_target)
        return html, record

    pipeline.raw_store.load_fetch_html = load_with_duplicate_target

    result = pipeline.parse_wave(
        str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
    )

    assert result.claimed == result.parsed == 2
    assert generic.batch_sizes == typed.batch_sizes == []
    assert len(generic.pages) == 2
    assert len(control.observation_claim_calls) == 2


def test_match_batch_size_and_cell_limits_are_inclusive(tmp_path):
    pipeline, _control, _records, generic, typed = (
        _pipeline_with_saved_matches(tmp_path)
    )
    pipeline.batch_persist_matches = 2
    pipeline.batch_persist_max_cells = 1000
    pipeline._match_item_cells = lambda _item: 500

    result = pipeline.parse_wave(
        str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
    )

    assert result.parsed == 2
    assert generic.batch_sizes == typed.batch_sizes == [2]

    overflow, _control, _records, generic, typed = _pipeline_with_saved_matches(
        tmp_path / "overflow"
    )
    overflow.batch_persist_max_cells = 1000
    overflow._match_item_cells = lambda _item: 600
    result = overflow.parse_wave(
        str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
    )
    assert result.parsed == 2
    assert generic.batch_sizes == typed.batch_sizes == [1, 1]


def test_match_batches_flush_around_non_match_items(tmp_path):
    pipeline, control, records, generic, typed = _pipeline_with_saved_matches(
        tmp_path
    )
    player = PageTarget(
        source="fbref",
        page_kind="player",
        target_id="fbref:player:1234abcd",
        canonical_url="https://fbref.com/en/players/1234abcd/x",
        source_ids={"player_id": "1234abcd"},
    )
    refresh, player_record = _commit_for_parse(
        pipeline.raw_store,
        player,
        "<html><head><link rel='canonical' "
        "href='https://fbref.com/en/players/1234abcd/x'>"
        "<meta property='og:type' content='Athlete'></head>"
        "<body><div id='meta'><h1>X</h1></div></body></html>",
    )
    control.frontier[player.target_id] = {
        "state": "fetched",
        "last_content_hash": player_record.content_hash,
        "last_logical_refresh_id": refresh,
    }
    player_fetch = {
        "target_id": player.target_id,
        "page_kind": "player",
        "logical_refresh_id": refresh,
        "content_hash": player_record.content_hash,
    }
    control.fetches = [control.fetches[0], player_fetch, control.fetches[1]]

    result = pipeline.parse_wave(
        str(uuid.uuid4()),
        page_kinds=["match", "player"],
        settings=_settings(),
    )

    assert result.parsed == 3
    assert generic.batch_sizes == typed.batch_sizes == [1, 1]
    assert [page.target_id for page, _ in generic.pages] == [
        records[0].target_id,
        player.target_id,
        records[1].target_id,
    ]


def test_batch_guards_enter_sorted_and_exit_reverse_sorted(tmp_path):
    pipeline, control, _records, _generic, _typed = (
        _pipeline_with_saved_matches(tmp_path)
    )
    control.fetches.reverse()

    pipeline.parse_wave(
        str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
    )

    entries = [
        event.removeprefix("content_guard:")
        for event in control.events
        if event.startswith("content_guard:")
    ]
    exits = [
        event.removeprefix("content_guard_exit:")
        for event in control.events
        if event.startswith("content_guard_exit:")
    ]
    assert entries == sorted(entries)
    assert exits == list(reversed(entries))


def test_batch_latest_same_hash_new_refresh_is_stale_at_final_lock(tmp_path):
    pipeline, control, records, generic, typed = _pipeline_with_saved_matches(
        tmp_path
    )
    original_batch = generic.persist_pages

    def race_after_preparation(items):
        result = original_batch(items)
        first = records[0]
        # A -> B -> A: same final content hash, different observation identity.
        control.frontier[first.target_id].update(
            last_content_hash="b" * 64,
            last_logical_refresh_id=str(uuid.uuid4()),
        )
        control.frontier[first.target_id]["last_content_hash"] = (
            first.content_hash
        )
        return result

    generic.persist_pages = race_after_preparation

    result = pipeline.parse_wave(
        str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
    )

    assert result.parsed == 2
    assert result.typed_promoted == 1
    assert result.stale_typed_observations_skipped == 1
    assert typed.batch_sizes == [1]
    stale = next(
        item for item in control.manifests
        if item["target_id"] == records[0].target_id
        and item["dataset"] == "typed:__stale_observation__"
    )
    assert stale["persistence_status"] == "skipped"
    assert not any(
        item["target_id"] == records[0].target_id
        and item["dataset"] == "typed:__complete__"
        for item in control.manifests
    )


def test_batch_none_verdict_fails_lease_after_unlock_without_markers(tmp_path):
    pipeline, control, records, _generic, typed = _pipeline_with_saved_matches(
        tmp_path
    )
    deferred = records[0]
    control.frontier[deferred.target_id]["state"] = "leased"

    with pytest.raises(ParseWaveError, match="TypedPromotionDeferred"):
        pipeline.parse_wave(
            str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
        )

    deferred_manifests = [
        item for item in control.manifests
        if item["target_id"] == deferred.target_id
    ]
    assert not any(
        item["dataset"] in {
            "typed:__stale_observation__",
            "typed:__complete__",
            "__page__",
        }
        for item in deferred_manifests
    )
    key = next(
        key for key, row in control.observations.items()
        if row["target_id"] == deferred.target_id
    )
    assert control.observations[key]["status"] == "failed"
    assert typed.batch_sizes == [1]
    fail = control.events.index(f"observation_fail:{deferred.target_id}")
    exits = [
        index for index, event in enumerate(control.events)
        if event.startswith("content_guard_exit:")
    ]
    assert max(exits) < fail


def test_batch_limit_flushes_at_max_items_and_oversized_singleton_falls_back(
    tmp_path,
):
    pipeline, _control, _records, generic, typed = _pipeline_with_saved_matches(
        tmp_path,
        match_ids=("0701e218", "a071faa8", "643d26fd"),
    )
    pipeline.batch_persist_matches = 2
    pipeline._match_item_cells = lambda _item: 500

    result = pipeline.parse_wave(
        str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
    )

    assert result.parsed == 3
    assert generic.batch_sizes == typed.batch_sizes == [2, 1]

    singleton, _control, _records, generic, typed = (
        _pipeline_with_saved_matches(
            tmp_path / "singleton", match_ids=("0701e218",)
        )
    )
    singleton.batch_persist_max_cells = 1000
    singleton._match_item_cells = lambda _item: 1001
    result = singleton.parse_wave(
        str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
    )
    assert result.parsed == 1
    assert generic.batch_sizes == typed.batch_sizes == []
    assert len(generic.pages) == len(typed.calls) == 1


def test_match_page_parser_error_keeps_legacy_sequential_error_evidence(
    tmp_path,
):
    pipeline, control, records, _generic, typed = _pipeline_with_saved_matches(
        tmp_path, match_ids=("0701e218",)
    )
    pipeline.generic_writer = ContractWriter()
    original_parse = pipeline._parse_generic

    def page_with_error(html, record):
        return replace(
            original_parse(html, record), errors=("forced_parser_error",)
        )

    pipeline._parse_generic = page_with_error

    with pytest.raises(ParseWaveError, match="generic contract failed"):
        pipeline.parse_wave(
            str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
        )

    page_evidence = [
        item for item in control.manifests
        if item["target_id"] == records[0].target_id
        and item["dataset"] == "__page__"
    ]
    assert len(page_evidence) == 1
    assert page_evidence[0]["parse_status"] == "failed"
    assert page_evidence[0]["persistence_status"] == "failed"
    assert typed.calls == []
    assert next(iter(control.observations.values()))["status"] == "failed"


@pytest.mark.parametrize(
    "fault_dataset",
    ["typed:match_events", "typed:__complete__", "__page__"],
)
def test_batch_per_item_manifest_fault_never_completes_early_and_continues(
    tmp_path, fault_dataset
):
    pipeline, control, records, _generic, _typed = (
        _pipeline_with_saved_matches(tmp_path)
    )
    first = records[0]
    original_record = control.record_dataset_manifest
    injected = False

    def fail_one_manifest(**kwargs):
        nonlocal injected
        if (
            not injected
            and kwargs["target_id"] == first.target_id
            and kwargs["dataset"] == fault_dataset
        ):
            injected = True
            raise RuntimeError(f"fault after {fault_dataset}")
        return original_record(**kwargs)

    control.record_dataset_manifest = fail_one_manifest

    with pytest.raises(ParseWaveError, match="fault after"):
        pipeline.parse_wave(
            str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
        )

    first_key = next(
        key for key, row in control.observations.items()
        if row["target_id"] == first.target_id
    )
    second_key = next(
        key for key, row in control.observations.items()
        if row["target_id"] == records[1].target_id
    )
    assert control.observations[first_key]["status"] == "failed"
    assert control.observations[second_key]["status"] == "succeeded"
    first_datasets = [
        item["dataset"] for item in control.manifests
        if item["target_id"] == first.target_id
    ]
    if fault_dataset != "__page__":
        assert "__page__" in first_datasets
        assert next(
            item for item in control.manifests
            if item["target_id"] == first.target_id
            and item["dataset"] == "__page__"
        )["validation_status"] == "failed"
    assert f"observation_complete:{first.target_id}" not in control.events
    failure = control.events.index(f"observation_fail:{first.target_id}")
    guard_exits = [
        index for index, event in enumerate(control.events)
        if event.startswith("content_guard_exit:")
    ]
    assert max(guard_exits) < failure


def test_batch_observation_completion_fault_fails_only_that_lease(tmp_path):
    pipeline, control, records, _generic, _typed = (
        _pipeline_with_saved_matches(tmp_path)
    )
    first = records[0]
    original_complete = control.complete_observation_processing

    def fail_before_complete(lease, **kwargs):
        if lease.target_id == first.target_id:
            raise RuntimeError("observation commit fault")
        return original_complete(lease, **kwargs)

    control.complete_observation_processing = fail_before_complete

    with pytest.raises(ParseWaveError, match="observation commit fault"):
        pipeline.parse_wave(
            str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
        )

    statuses = {
        row["target_id"]: row["status"] for row in control.observations.values()
    }
    assert statuses[first.target_id] == "failed"
    assert statuses[records[1].target_id] == "succeeded"
    assert not any(row["status"] == "processing" for row in control.observations.values())
    failure = control.events.index(f"observation_fail:{first.target_id}")
    assert max(
        index for index, event in enumerate(control.events)
        if event.startswith("content_guard_exit:")
    ) < failure


def test_generic_batch_misaligned_counts_use_idempotent_sequential_repair(
    tmp_path,
):
    pipeline, control, _records, generic, typed = (
        _pipeline_with_saved_matches(tmp_path)
    )
    original_batch = generic.persist_pages

    def misaligned(items):
        counts = original_batch(items)
        return counts[:-1]

    generic.persist_pages = misaligned

    result = pipeline.parse_wave(
        str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
    )

    assert result.parsed == 2
    assert len(control.observation_claim_calls) == 2
    # First batch writes plus one idempotent singleton repair per item.
    assert len(generic.pages) == 4
    assert typed.batch_sizes == []
    assert [call[0] for call in typed.calls] == ["match", "match"]


def test_second_batch_guard_enter_fault_closes_first_then_fails_all_leases(
    tmp_path,
):
    pipeline, control, records, _generic, typed = (
        _pipeline_with_saved_matches(tmp_path)
    )
    ordered = sorted(record.target_id for record in records)

    @contextmanager
    def guard_with_second_enter_fault(
        target_id, content_hash, logical_refresh_id
    ):
        del content_hash, logical_refresh_id
        control.events.append(f"content_guard:{target_id}")
        if target_id == ordered[1]:
            raise RuntimeError("second guard enter fault")
        try:
            yield True
        except RuntimeError:
            control.events.append(f"content_guard_rollback:{target_id}")
            raise
        finally:
            control.events.append(f"content_guard_exit:{target_id}")

    control.guard_latest_content = guard_with_second_enter_fault

    with pytest.raises(ParseWaveError, match="second guard enter fault"):
        pipeline.parse_wave(
            str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
        )

    first_exit = control.events.index(f"content_guard_exit:{ordered[0]}")
    rollback = control.events.index(
        f"content_guard_rollback:{ordered[0]}"
    )
    failures = [
        control.events.index(f"observation_fail:{target_id}")
        for target_id in ordered
    ]
    assert rollback < first_exit
    assert all(first_exit < failure for failure in failures)
    assert typed.calls == []
    assert typed.batch_sizes == []
    assert {
        row["status"] for row in control.observations.values()
    } == {"failed"}


@pytest.mark.parametrize(
    "fault_stage",
    [TABLE_CELLS_TABLE, TABLE_INVENTORY_TABLE, PAGE_MANIFEST_TABLE],
)
def test_generic_partial_commit_repair_matches_sequential_durable_state(
    tmp_path, durable_sequential_baseline, fault_stage
):
    repaired = _durable_replay_case(
        tmp_path / "repaired",
        batch=True,
        generic_fault_stage=fault_stage,
    )

    assert repaired.snapshot == durable_sequential_baseline.snapshot
    assert repaired.raw_loads == repaired.observation_claims == 2
    assert repaired.same_generic_objects is True
    assert repaired.fault_was_durable is True
    assert repaired.network_calls == 0


@pytest.mark.parametrize(
    "fault_stage",
    [*MATCH_DATASET_TABLES.values(), MATCH_AVAILABILITY_TABLE],
)
def test_typed_partial_commit_repair_matches_sequential_durable_state(
    tmp_path, durable_sequential_baseline, fault_stage
):
    repaired = _durable_replay_case(
        tmp_path / "repaired",
        batch=True,
        typed_fault_stage=fault_stage,
    )

    assert repaired.snapshot == durable_sequential_baseline.snapshot
    assert repaired.raw_loads == repaired.observation_claims == 2
    assert repaired.same_typed_objects is True
    assert repaired.fault_was_durable is True
    assert repaired.guard_entries == 2
    assert repaired.network_calls == 0


def test_typed_batch_count_misalignment_repairs_same_objects_under_guards(
    tmp_path, durable_sequential_baseline
):
    repaired = _durable_replay_case(
        tmp_path / "repaired", batch=True, typed_misalign=True
    )

    assert repaired.snapshot == durable_sequential_baseline.snapshot
    assert repaired.raw_loads == repaired.observation_claims == 2
    assert repaired.same_typed_objects is True
    assert repaired.guard_entries == 2
    assert repaired.network_calls == 0


def test_guard_exit_fault_after_completion_is_loud_and_replay_claims_nothing(
    tmp_path,
):
    pipeline, control, records = _pipeline_with_small_durable_matches(tmp_path)
    generic = DurableGenericWriter()
    typed = DurableTypedWriter()
    pipeline.generic_writer = generic
    pipeline.typed_adapter = FakeTypedAdapter(typed)
    ordered = sorted(record.target_id for record in records)
    faulted = False

    @contextmanager
    def guard_with_exit_fault(target_id, content_hash, logical_refresh_id):
        nonlocal faulted
        control.events.append(f"content_guard:{target_id}")
        frontier = control.frontier[target_id]
        verdict = (
            frontier["last_content_hash"] == content_hash
            and frontier["last_logical_refresh_id"] == logical_refresh_id
        )
        try:
            yield verdict
        finally:
            control.events.append(f"content_guard_exit:{target_id}")
            if target_id == ordered[1] and not faulted:
                faulted = True
                raise RuntimeError("guard exit commit fault")

    control.guard_latest_content = guard_with_exit_fault
    raw_loads = 0
    original_load = pipeline.raw_store.load_fetch_html

    def counted_load(logical_refresh_id):
        nonlocal raw_loads
        raw_loads += 1
        return original_load(logical_refresh_id)

    pipeline.raw_store.load_fetch_html = counted_load

    with pytest.raises(ParseWaveError, match="guard exit commit fault"):
        pipeline.parse_wave(
            str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
        )

    before_replay = _stable({
        "generic": generic.snapshot(),
        "typed": typed.snapshot(),
        "control": _normalized_control_state(control),
    })
    assert {
        row["status"] for row in control.observations.values()
    } == {"succeeded"}
    assert not any(
        event.startswith("observation_fail:") for event in control.events
    )
    assert raw_loads == len(control.observation_claim_calls) == 2

    replay = pipeline.parse_wave(
        str(uuid.uuid4()), page_kinds=["match"], settings=_settings()
    )
    after_replay = _stable({
        "generic": generic.snapshot(),
        "typed": typed.snapshot(),
        "control": _normalized_control_state(control),
    })

    assert replay.cohort_size == replay.claimed == replay.parsed == 0
    assert raw_loads == len(control.observation_claim_calls) == 2
    assert after_replay == before_replay


def test_completed_manifest_conflict_does_not_mask_processing_failure(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="player",
        canonical_url="https://fbref.com/en/players/1234abcd/Player",
        source_ids={"player_id": "1234abcd"},
    ))
    refresh, record = _commit_for_parse(
        raw,
        target,
        """
        <table id="stats_standard"><tr>
          <th data-stat="player">Player</th></tr>
          <tr><td data-stat="player">Player</td></tr></table>
        """,
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

    class FailingWriter:
        def persist_page(self, *_args, **_kwargs):
            raise RuntimeError("generic write failed")

    def immutable_manifest(**_kwargs):
        raise StateConflict("completed manifest is immutable")

    control.record_dataset_manifest = immutable_manifest
    pipeline = FBrefPipeline(control, raw, generic_writer=FailingWriter())

    with pytest.raises(ParseWaveError) as captured:
        pipeline.parse_wave(
            str(uuid.uuid4()),
            page_kinds=["player"],
            settings=_settings(),
        )

    assert "generic write failed" in str(captured.value)
    assert "completed manifest is immutable" not in str(captured.value)


def test_cross_run_recovery_processes_raw_from_failed_source_run_offline(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="player",
        canonical_url="https://fbref.com/en/players/1234abcd/Player",
        source_ids={"player_id": "1234abcd"},
    ))
    refresh, record = _commit_for_parse(
        raw,
        target,
        """
        <table id="stats_standard"><tr>
          <th data-stat="player">Player</th></tr>
          <tr><td data-stat="player">Player</td></tr></table>
        """,
    )
    source_run_id = str(uuid.uuid4())
    control.frontier[target.target_id] = {
        "target_id": target.target_id,
        "state": "fetched",
        "last_content_hash": record.content_hash,
    }
    control.fetches = [{
        "run_id": source_run_id,
        "source_run_type": "current",
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "logical_refresh_id": refresh,
        "content_hash": record.content_hash,
    }]

    def forbidden_transport(*_args):
        raise AssertionError("raw recovery cannot construct a transport")

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=forbidden_transport,
    )
    result = pipeline.recover_unprocessed_wave(
        str(uuid.uuid4()),
        page_kinds=["player"],
        settings=_settings("current"),
    )

    assert result.parsed == 1
    key = (
        refresh,
        PAGE_DOCUMENT_VERSION,
        TYPED_BRONZE_PARSER_VERSION,
        DISCOVERY_PARSER_VERSION,
    )
    assert control.observations[key]["status"] == "succeeded"


def test_page_v3_recovers_verified_tableless_player_from_v2_raw_offline(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="player",
        canonical_url=(
            "https://fbref.com/en/players/406c5597/Naime-Said-Mchindra"
        ),
        source_ids={"player_id": "406c5597"},
    ))
    html = """
    <html><head>
      <link rel="canonical"
        href="https://fbref.com/en/players/406c5597/Naime-Said-Mchindra">
      <meta property="og:url"
        content="https://fbref.com/en/players/406c5597/Naime-Said-Mchindra">
      <meta property="og:type" content="Athlete">
    </head><body><div id="meta"><h1>Naime Said Mchindra</h1></div>
    </body></html>
    """
    refresh, record = _commit_for_parse(raw, target, html)
    source_run_id = str(uuid.uuid4())
    control.frontier[target.target_id] = {
        "target_id": target.target_id,
        "state": "fetched",
        "last_content_hash": record.content_hash,
    }
    old_key = (
        refresh,
        "fbref-page-document-v2",
        TYPED_BRONZE_PARSER_VERSION,
        DISCOVERY_PARSER_VERSION,
    )
    control.observations[old_key] = {"status": "succeeded"}
    control.fetches = [{
        "run_id": source_run_id,
        "source_run_type": "current",
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "logical_refresh_id": refresh,
        "content_hash": record.content_hash,
    }]

    def forbidden_transport(*_args):
        raise AssertionError("raw recovery cannot construct a transport")

    writer = ContractWriter()
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=writer,
        fetcher_factory=forbidden_transport,
    )
    result = pipeline.recover_unprocessed_wave(
        str(uuid.uuid4()),
        page_kinds=["player"],
        settings=_settings("current"),
    )

    assert PAGE_DOCUMENT_VERSION == "fbref-page-document-v4"
    assert result.parsed == 1
    assert writer.pages[0][0].tables == ()
    assert writer.pages[0][0].errors == ()
    new_key = (
        refresh,
        PAGE_DOCUMENT_VERSION,
        TYPED_BRONZE_PARSER_VERSION,
        DISCOVERY_PARSER_VERSION,
    )
    assert control.observations[old_key]["status"] == "succeeded"
    assert control.observations[new_key]["status"] == "succeeded"
    completion = next(
        item for item in control.manifests if item["dataset"] == "__page__"
    )
    assert completion["availability"] == "empty"
    assert completion["row_count"] == 0


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


def test_seed_links_persists_one_ordered_batch_per_observation(tmp_path):
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
    parent = page_target_from_link(DiscoveredPageLink(
        page_kind="schedule",
        canonical_url=(
            "https://fbref.com/en/comps/9/2025-2026/schedule/x"
        ),
        source_ids={"competition_id": "9", "season_id": "2025-2026"},
    ))
    _, parent_record = _commit_for_parse(raw, parent, "<html></html>")
    links = [
        DiscoveredPageLink(
            page_kind="player",
            canonical_url=(
                f"https://fbref.com/en/players/{index:08x}/Player"
            ),
            source_ids={
                "player_id": f"{index:08x}",
                "competition_id": "9",
                "season_id": "2025-2026",
            },
        )
        for index in reversed(range(50))
    ]
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
    )

    seeded, skipped = pipeline._seed_links(
        links,
        historical=False,
        parent_record=parent_record,
    )

    assert (seeded, skipped) == (50, 0)
    assert control.eligible_competition_calls == 1
    assert len(control.frontier_batches) == 1
    targets, provenance = control.frontier_batches[0]
    assert len(targets) == len(provenance) == 50
    assert [target.target_id for target in targets] == sorted(
        target.target_id for target in targets
    )
    assert control.events.index("frontier_batch:end") < control.events.index(
        "scope_reconcile"
    )


def test_schedule_seeds_50_mixed_matches_in_one_frontier_batch(tmp_path):
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
    rows = "".join(
        f"""
        <tr><th data-stat="date">2026-01-{index % 28 + 1:02d}</th>
          <td data-stat="home_team">Home {index}</td>
          <td data-stat="away_team">Away {index}</td>
          <td data-stat="score">{"1–0" if index < 25 else ""}</td>
          <td data-stat="match_report"><a
            href="/en/matches/{index:08x}/Match-{index}">Report</a></td>
        </tr>
        """
        for index in range(50)
    )
    html = f"""
    <table id="sched_all"><thead><tr>
      <th data-stat="date">Date</th>
      <th data-stat="home_team">Home</th>
      <th data-stat="away_team">Away</th>
      <th data-stat="score">Score</th>
      <th data-stat="match_report">Report</th>
    </tr></thead><tbody>{rows}</tbody></table>
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
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        typed_adapter=FakeTypedAdapter(FakeTypedWriter()),
    )
    seed_eligible_reads = []
    seed_candidates = pipeline._seed_link_candidates

    def measured_seed(*args, **kwargs):
        before = control.eligible_competition_calls
        result = seed_candidates(*args, **kwargs)
        seed_eligible_reads.append(
            control.eligible_competition_calls - before
        )
        return result

    pipeline._seed_link_candidates = measured_seed

    result = pipeline.parse_wave(
        str(uuid.uuid4()),
        page_kinds=["schedule"],
        settings=_settings("current"),
    )

    assert (result.seeded, result.skipped_ineligible) == (50, 0)
    assert seed_eligible_reads == [1]
    assert len(control.frontier_batches) == 1
    targets, provenance = control.frontier_batches[0]
    assert len(targets) == len(provenance) == 50
    policies = {
        target.source_ids["match_id"]: target.refresh_policy
        for target in targets
    }
    assert set(policies.values()) == {"current_completed_once", "daily"}
    assert all(
        policies[f"{index:08x}"] == (
            "current_completed_once" if index < 25 else "daily"
        )
        for index in range(50)
    )
    assert control.events.count("scope_reconcile") == 1
    assert control.events.index("frontier_batch:end") < control.events.index(
        "scope_reconcile"
    )


def test_fetch_wave_reserves_budget_and_commits_raw_before_control(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    html = b"<html><table id='comps'><tr><td>x</td></tr></table></html>"
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda *_: FakeFetcher(
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


def test_browser_failure_advances_throttle_before_the_next_clearance(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)

    class BrowserFailureFetcher:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def ensure_clearance(self):
            control.events.append("browser_failure")
            raise FetchError(
                "browser export failed",
                error_class="clearance_failed",
                browser_requests=1,
                browser_bootstrap_attempts=1,
            )

    fetchers = iter(
        (
            BrowserFailureFetcher(),
            FakeFetcher(control.events, b"<html>ok</html>"),
        )
    )
    sleeps = []
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda *_: next(fetchers),
        sleep=lambda seconds: sleeps.append(seconds),
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        str(uuid.UUID(int=1)),
        worker_id="worker-1",
        page_kinds=["competition_index"],
        settings=_settings(),
    )

    assert result.fetched == 1
    throttles = [
        index
        for index, event in enumerate(control.events)
        if event == "throttle"
    ]
    assert len(throttles) == 5
    assert control.events.index("browser_failure") < throttles[1]
    assert throttles[1] < throttles[2] < control.events.index("browser")
    assert control.events.index("browser") < throttles[3] < throttles[4]
    assert sleeps == []


def test_browser_bootstrap_and_warm_targets_have_separate_rate_slots(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    run_id = str(uuid.UUID(int=1))

    def lease(number):
        return TargetLease(
            attempt_id=str(uuid.UUID(int=20 + number)),
            run_id=run_id,
            target_id=f"fbref:competition:{number}",
            logical_refresh_id=str(uuid.UUID(int=30 + number)),
            canonical_url=(
                f"https://fbref.com/en/comps/{number}/history/x-Seasons"
            ),
            page_kind="competition",
            source_ids={"competition_id": str(number)},
            claim_token=str(uuid.UUID(int=40 + number)),
            lease_epoch=1,
            attempt_number=1,
            leased_by="worker-1",
            lease_expires_at=NOW + timedelta(minutes=10),
        )

    control.claim_targets = lambda *args, **kwargs: [lease(9), lease(12)]
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda *_: FakeFetcher(
            control.events, b"<html>ok</html>"
        ),
        sleep=lambda seconds: control.events.append(f"sleep:{seconds}"),
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        run_id,
        worker_id="worker-1",
        page_kinds=["competition"],
        settings=_settings(),
    )

    assert result.fetched == 2
    throttles = [
        index
        for index, event in enumerate(control.events)
        if event == "throttle"
    ]
    requests = [
        index for index, event in enumerate(control.events) if event == "http"
    ]
    browser = control.events.index("browser")
    assert len(throttles) == 4
    assert len(requests) == 2
    assert throttles[0] < browser < throttles[1] < throttles[2]
    assert throttles[2] < requests[0] < throttles[3] < requests[1]
    assert control.events.count("browser") == 1
    assert not any(event.startswith("sleep:") for event in control.events)


def test_fetch_wave_settles_exact_provider_bytes_not_geoip_reserve(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    html = b"<html><table id='comps'><tr><td>x</td></tr></table></html>"

    class MeteredFetcher(FakeFetcher):
        def fetch(self, url, **kwargs):
            return replace(
                super().fetch(url, **kwargs),
                browser_unobserved_bytes=GEOIP_BYTE_RESERVATION_BYTES,
                provider_billed_bytes=321,
            )

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda *_: MeteredFetcher(control.events, html),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    pipeline.fetch_wave(
        str(uuid.UUID(int=1)),
        worker_id="worker-1",
        page_kinds=["competition_index"],
        settings=_settings(),
    )

    assert control.settlements[0][1]["bytes_used"] == 321
    assert control.session_metrics[0][1]["browser_unobserved_bytes"] == (
        GEOIP_BYTE_RESERVATION_BYTES
    )
    assert control.session_metrics[0][1]["provider_billed_bytes"] == 321


def test_default_off_raw_store_failure_still_settles_authoritative_meter(
    tmp_path,
    monkeypatch,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)

    class MeteredFetcher(FakeFetcher):
        def fetch(self, url, **kwargs):
            return replace(
                super().fetch(url, **kwargs),
                provider_billed_bytes=321,
            )

    monkeypatch.setattr(
        raw,
        "commit_fetch",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            OSError("raw store unavailable")
        ),
    )
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda *_: MeteredFetcher(
            control.events, b"<html>ok</html>"
        ),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    with pytest.raises(FetchWaveError, match="OSError"):
        pipeline.fetch_wave(
            str(uuid.UUID(int=1)),
            worker_id="worker-1",
            page_kinds=["competition_index"],
            settings=_settings(),
        )

    assert control.settlements[0][1]["bytes_used"] == 321
    assert len(control.session_metrics) == 1
    assert control.session_metrics[0][1]["provider_billed_bytes"] == 321
    assert control.events.index("settle") < control.events.index("fail")


def test_fetch_wave_persists_retry_failure_evidence_and_exact_request_count(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda *_: FakeFailingFetcher(control.events),
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
    assert control.events.count("throttle") == 4
    assert control.events.index("http") < control.events.index(
        "throttle", control.events.index("http") + 1
    )
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
        "requeue": False,
        "http_status": 500,
        "http_request_count": 2,
        "http_status_history": (500, 500),
        "wire_bytes": 303,
        "provider_billed_bytes": None,
        "latency_ms": 321,
        "transport_version": FETCHER_VERSION,
        "session_version": str(uuid.UUID(int=7)),
    }


def test_generic_fetch_exception_settles_and_terminates_attempt(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)

    class ExplodingFetcher:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def ensure_clearance(self):
            return True

        def fetch(self, *args, **kwargs):
            raise RuntimeError("driver exploded")

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda *_: ExplodingFetcher(),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    with pytest.raises(FetchWaveError, match="RuntimeError"):
        pipeline.fetch_wave(
            str(uuid.UUID(int=1)),
            worker_id="worker-1",
            page_kinds=["competition_index"],
            settings=_settings(),
        )

    assert control.settlements[0][1] == {
        "requests_used": 0,
        "bytes_used": 0,
    }
    assert len(control.failed) == 1
    assert control.failed[0][1]["error_class"] == "RuntimeError"
    assert control.failed[0][1]["error_message"] == "driver exploded"
    assert control.events.index("settle") < control.events.index("fail")


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

    def forbidden(*_):
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


def test_persistent_rollover_precedes_raw_recovery_early_exit(tmp_path):
    raw = _raw_store(tmp_path)
    control = PersistentFakeControl(raw)
    target = competition_index_target()
    raw.commit_fetch(
        target,
        b"<html>committed</html>",
        logical_refresh_id=str(uuid.UUID(int=2)),
        attempt_id=str(uuid.UUID(int=99)),
        http_status=200,
    )

    class DueFetcher:
        persistent_http_session = True

        def persistent_session_rollover_due(self, *, within_seconds=0):
            control.events.append(("rollover_check", within_seconds))
            return True

        def finalize_metered_session(self):
            control.events.append("provider_finalize")
            return PersistentMeteredSessionReceipt(
                session_id="session-existing",
                meter="proxy_filter_provider_path_v2",
                baseline_provider_bytes=0,
                page_provider_bytes=0,
                authoritative_provider_bytes=0,
                tail_provider_bytes=0,
            )

        def reset_clearance(self):
            control.events.append("rollover_reset")

    live = _LiveFetchSession(
        fetcher=DueFetcher(),
        session_id="session-existing",
        needs_clearance=False,
        persistent_enabled=True,
        state="active",
        tail_reserved=True,
    )
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda *_: (_ for _ in ()).throw(
            AssertionError("raw recovery must not build a transport")
        ),
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        str(uuid.UUID(int=1)),
        worker_id="worker-1",
        page_kinds=["competition_index"],
        settings=_persistent_settings(),
        _live_session=live,
    )

    assert result.recovered_from_raw == 1
    assert control.events.index("provider_finalize") < control.events.index(
        "complete"
    )
    assert control.events.index("session_close") < control.events.index(
        "complete"
    )
    assert "rollover_reset" in control.events


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
        fetcher_factory=lambda *_: FakeFetcher(
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
def test_current_completed_transition_refreshes_instead_of_adopting_prior_raw(
    tmp_path,
    raw_version,
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
        "refresh_policy": "current_completed_once",
        "state": "queued",
    }

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda *_: FakeFetcher(control.events, fresh),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )
    result = pipeline.fetch_wave(
        run_id,
        worker_id="worker-1",
        page_kinds=["match"],
        settings=_settings("current"),
    )

    committed_body, committed = raw.load_fetch(refresh_id)
    assert committed_body == fresh
    assert committed.imported_from_manifest_key is None
    assert result.fetched == 1
    assert result.recovered_from_raw == 0
    assert result.requests == 2
    assert "reserve" in control.events
    assert "http" in control.events


@pytest.mark.parametrize("raw_version", ["v1", "prior-v2"])
def test_historical_once_adopts_verified_prior_raw_without_network(
    tmp_path,
    raw_version,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    run_id = str(uuid.UUID(int=1))
    refresh_id = str(uuid.UUID(int=20))
    target = match_page_target("a071faa8")
    historical_body = b"<html>immutable-history</html>"
    if raw_version == "v1":
        raw.store_html(target, historical_body.decode("utf-8"))
    else:
        raw.commit_fetch(
            target,
            historical_body,
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
        "refresh_policy": "historical_once",
        "state": "queued",
    }

    def unexpected_fetcher(*_args):
        raise AssertionError("historical raw reuse must not open a transport")

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=unexpected_fetcher,
        sleep=lambda _: None,
        clock=lambda: NOW,
    )
    result = pipeline.fetch_wave(
        run_id,
        worker_id="worker-1",
        page_kinds=["match"],
        settings=_settings("backfill"),
    )

    committed_body, committed = raw.load_fetch(refresh_id)
    assert committed_body == historical_body
    assert committed.imported_from_manifest_key is not None
    assert result.fetched == 0
    assert result.recovered_from_raw == 1
    assert result.requests == 0
    assert "reserve" not in control.events
    assert "http" not in control.events


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
        fetcher_factory=lambda *_: FakeFetcher(control.events, fresh),
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

    def forbidden(*_):
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
        control, raw, generic_writer=FakeWriter(), fetcher_factory=lambda *_: None
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


@pytest.mark.parametrize(
    "run_type",
    ["current", "backfill"],
)
def test_single_match_competition_inventories_current_and_backfill_targets(
    tmp_path, run_type,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.registry["602"] = {
        "competition_id": "602",
        "canonical_url": "https://fbref.com/en/comps/602/history/x",
        "name": "Super Cup",
        "gender": "male",
        "classification": "cup:club",
        "metadata": {"last_season": "2025"},
    }
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="competition",
        canonical_url="https://fbref.com/en/comps/602/history/x",
        source_ids={"competition_id": "602"},
    ))
    html = """
    <table id="seasons"><tbody>
      <tr><th data-stat="season">
          <a href="/en/matches/abcdef12/Super-Cup-Final">2025</a>
        </th><td data-stat="champion">Winner</td></tr>
      <tr><th data-stat="season">
          <a href="/en/matches/98765432/Old-Super-Cup-Final">2024</a>
        </th><td data-stat="champion">Old winner</td></tr>
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
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    result = pipeline.parse_wave(
        str(uuid.uuid4()),
        page_kinds=["competition"],
        settings=_settings(run_type),
    )

    match_targets = [
        row for row in control.frontier.values()
        if row["page_kind"] == "match"
    ]
    assert result.seeded == 2
    assert len(match_targets) == 2
    by_match_id = {
        row["source_ids"]["match_id"]: row for row in match_targets
    }
    assert by_match_id["abcdef12"]["source_ids"] == {
        "competition_id": "602",
        "season_id": "2025",
        "match_id": "abcdef12",
    }
    assert by_match_id["abcdef12"]["refresh_policy"] == (
        "current_completed_once"
    )
    assert by_match_id["98765432"]["source_ids"] == {
        "competition_id": "602",
        "season_id": "2024",
        "match_id": "98765432",
    }
    assert by_match_id["98765432"]["refresh_policy"] == "historical_once"
    assert {entry.season_id for entry in control.seasons} == {"2024", "2025"}
    assert {
        entry.season_id for entry in control.seasons if entry.is_current
    } == {"2025"}
    assert all(
        entry.metadata.get("direct_match_only") is True
        for entry in control.seasons
    )


@pytest.mark.parametrize(
    ("run_type", "seeded_season", "season_policy"),
    [
        ("current", "2025", "daily"),
        ("backfill", "2024", "historical_once"),
    ],
)
def test_competition_history_aggregates_seasons_and_direct_matches(
    tmp_path, run_type, seeded_season, season_policy
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.registry["602"] = {
        "competition_id": "602",
        "canonical_url": "https://fbref.com/en/comps/602/history/x",
        "name": "Super Cup",
        "gender": "male",
        "classification": "cup:club",
        "metadata": {"last_season": "2025"},
    }
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="competition",
        canonical_url="https://fbref.com/en/comps/602/history/x",
        source_ids={"competition_id": "602"},
    ))
    html = """
    <table id="seasons"><tbody>
      <tr><th data-stat="season"><a
        href="/en/comps/602/2025/current">2025</a></th></tr>
      <tr><th data-stat="season"><a
        href="/en/comps/602/2024/old">2024</a></th></tr>
      <tr><th data-stat="season"><a
        href="/en/matches/abcdef12/Current-Final">2025</a></th></tr>
      <tr><th data-stat="season"><a
        href="/en/matches/98765432/Old-Final">2023</a></th></tr>
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
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    result = pipeline.parse_wave(
        str(uuid.uuid4()),
        page_kinds=["competition"],
        settings=_settings(run_type),
    )

    assert (result.seeded, result.skipped_ineligible) == (3, 0)
    assert control.eligible_competition_calls == 1
    assert len(control.frontier_batches) == 1
    targets, provenance = control.frontier_batches[0]
    assert len(targets) == len(provenance) == 3
    season_target = next(
        target for target in targets if target.page_kind == "season"
    )
    assert season_target.source_ids["season_id"] == seeded_season
    assert season_target.refresh_policy == season_policy
    match_policies = {
        target.source_ids["match_id"]: target.refresh_policy
        for target in targets if target.page_kind == "match"
    }
    assert match_policies == {
        "abcdef12": "current_completed_once",
        "98765432": "historical_once",
    }
    assert control.events.count("scope_reconcile") == 1
    assert control.events.index("frontier_batch:end") < control.events.index(
        "scope_reconcile"
    )


def test_card_grid_competition_passes_generic_then_semantic_contract(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.registry["255"] = {
        "competition_id": "255",
        "canonical_url": "https://fbref.com/en/comps/255/history/x",
        "name": "Inter-confederation play-offs",
        "gender": "male",
        "classification": "cup:national_team",
        "metadata": {"last_season": "2026"},
    }
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="competition",
        canonical_url="https://fbref.com/en/comps/255/history/x",
        source_ids={"competition_id": "255"},
    ))
    html = """
    <html><body><main><div class="content_grid">
      <a href="/en/comps/255/2026/2026-Play-offs-Stats">2026</a>
    </div></main></body></html>
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
    writer = ContractWriter()
    pipeline = FBrefPipeline(control, raw, generic_writer=writer)

    result = pipeline.parse_wave(
        str(uuid.uuid4()),
        page_kinds=["competition"],
        settings=_settings("current"),
    )

    assert result.parsed == 1
    assert writer.pages[0][0].tables == ()
    seasons = [
        row for row in control.frontier.values()
        if row["page_kind"] == "season"
    ]
    assert len(seasons) == 1
    assert seasons[0]["source_ids"]["season_id"] == "2026"


def test_empty_semantic_table_reaches_pipeline_writer_without_contract_error(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="match",
        canonical_url="https://fbref.com/en/matches/abcdef12/source",
        source_ids={"match_id": "abcdef12"},
    ))
    _, record = _commit_for_parse(
        raw,
        target,
        """
        <table id="shots_all"><thead><tr>
          <th data-stat="player">Player</th>
        </tr></thead><tbody></tbody></table>
        """,
    )
    writer = ContractWriter()
    pipeline = FBrefPipeline(control, raw, generic_writer=writer)

    page = pipeline._persist_generic(str(uuid.uuid4()), """
        <table id="shots_all"><thead><tr>
          <th data-stat="player">Player</th>
        </tr></thead><tbody></tbody></table>
        """, record)

    assert page.errors == ()
    assert page.tables[0].availability.value == "empty"


def test_single_match_season_zero_table_shape_reaches_not_applicable_semantics(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.registry["122"] = {
        "competition_id": "122",
        "canonical_url": "https://fbref.com/en/comps/122/history/x",
        "name": "UEFA Super Cup",
        "gender": "male",
        "classification": "cup:club",
        "metadata": {"last_season": "2013-2014"},
    }
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="season",
        canonical_url=(
            "https://fbref.com/en/comps/122/2013-2014/"
            "2013-UEFA-Super-Cup-Stats"
        ),
        source_ids={
            "competition_id": "122",
            "season_id": "2013-2014",
        },
    ))
    html = """
    <div id="content"><h1>2013 UEFA Super Cup Stats</h1>
      <a href="/en/comps/122/history/UEFA-Super-Cup-Seasons">Seasons</a>
    </div>
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
    writer = ContractWriter()
    typed_writer = FakeTypedWriter()
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=writer,
        typed_adapter=FakeTypedAdapter(typed_writer),
    )

    result = pipeline.parse_wave(
        str(uuid.uuid4()),
        page_kinds=["season"],
        settings=_settings("current"),
    )

    assert result.parsed == 1
    assert writer.pages[0][0].errors == ()
    assert writer.pages[0][0].tables == ()
    assert len(typed_writer.calls) == 1


def test_zero_table_source_shell_fails_before_typed_promotion(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="season",
        canonical_url=(
            "https://fbref.com/en/comps/122/2013-2014/"
            "2013-UEFA-Super-Cup-Stats"
        ),
        source_ids={
            "competition_id": "122",
            "season_id": "2013-2014",
        },
    ))
    html = "<html><body><p>temporary source shell</p></body></html>"
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
    generic_writer = ContractWriter()
    typed_writer = FakeTypedWriter()
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=generic_writer,
        typed_adapter=FakeTypedAdapter(typed_writer),
    )

    with pytest.raises(ParseWaveError, match="Season source contract failed"):
        pipeline.parse_wave(
            str(uuid.uuid4()),
            page_kinds=["season"],
            settings=_settings("current"),
        )

    assert generic_writer.pages[0][0].tables == ()
    assert typed_writer.calls == []
    # A shell that cannot prove its own identity may be a challenge page or a
    # truncated capture: fresher bytes can still parse, so it must never be
    # retired on this evidence.
    assert control.frontier[record.target_id]["state"] == "fetched"


def _schedule_less_season_wave(tmp_path):
    """Cohort of the production shape plus a healthy season page.

    A dead league's archived edition (NASL 2017) publishes squad tables but no
    Scores & Fixtures link at all, which no retry can change.
    """

    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    for competition_id, name in (("76", "NASL"), ("122", "UEFA Super Cup")):
        control.registry[competition_id] = {
            "competition_id": competition_id,
            "canonical_url": (
                f"https://fbref.com/en/comps/{competition_id}/history/x"
            ),
            "name": name,
            "gender": "male",
            "classification": "cup:club",
            "metadata": {},
        }
    rejected = page_target_from_link(DiscoveredPageLink(
        page_kind="season",
        canonical_url="https://fbref.com/en/comps/76/2017/2017-NASL-Stats",
        source_ids={"competition_id": "76", "season_id": "2017"},
    ))
    healthy = page_target_from_link(DiscoveredPageLink(
        page_kind="season",
        canonical_url=(
            "https://fbref.com/en/comps/122/2013-2014/"
            "2013-UEFA-Super-Cup-Stats"
        ),
        source_ids={"competition_id": "122", "season_id": "2013-2014"},
    ))
    pages = [
        (rejected, """
        <div id="content"><h1>2017 NASL Stats</h1>
          <a href="/en/comps/76/history/NASL-Seasons">Seasons</a>
          <table id="stats_squads_standard_for">
            <thead><tr><th data-stat="team">Squad</th></tr></thead>
            <tbody><tr><td data-stat="team">New York Cosmos</td></tr></tbody>
          </table>
        </div>
        """),
        (healthy, """
        <div id="content"><h1>2013 UEFA Super Cup Stats</h1>
          <a href="/en/comps/122/history/UEFA-Super-Cup-Seasons">Seasons</a>
        </div>
        """),
    ]
    records = {}
    for target, html in pages:
        refresh, record = _commit_for_parse(raw, target, html)
        records[target.target_id] = record
        control.frontier[record.target_id] = {
            "target_id": record.target_id,
            "page_kind": record.page_kind,
            "source_ids": dict(record.source_ids),
            "state": "fetched",
            "last_content_hash": record.content_hash,
        }
        control.fetches.append({
            "target_id": record.target_id,
            "page_kind": record.page_kind,
            "logical_refresh_id": refresh,
        })
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=ContractWriter(),
        typed_adapter=FakeTypedAdapter(FakeTypedWriter()),
    )
    return control, pipeline, rejected.target_id, healthy.target_id


def test_schedule_less_season_page_is_retired_without_failing_its_cohort(
    tmp_path,
):
    control, pipeline, rejected_id, healthy_id = _schedule_less_season_wave(
        tmp_path
    )

    result = pipeline.recover_unprocessed_wave(
        str(uuid.uuid4()),
        page_kinds=["season"],
        settings=_settings("current"),
    )

    assert result.failures == []
    assert result.contract_quarantined == 1
    # The healthy sibling in the same cohort is not held hostage.
    assert result.parsed == 1
    assert control.frontier[healthy_id]["state"] == "fetched"
    rejected = control.frontier[rejected_id]
    assert rejected["state"] == "quarantined"
    assert rejected["last_error_class"] == "ParseContractQuarantined"
    assert rejected["last_error_message"] == "schedule_link_missing"
    assert rejected["next_fetch_at"] is None


def test_contract_quarantine_drops_the_target_from_the_recovery_cohort(
    tmp_path,
):
    control, pipeline, rejected_id, _ = _schedule_less_season_wave(tmp_path)

    first = pipeline.recover_unprocessed_wave(
        str(uuid.uuid4()),
        page_kinds=["season"],
        settings=_settings("current"),
    )
    second = pipeline.recover_unprocessed_wave(
        str(uuid.uuid4()),
        page_kinds=["season"],
        settings=_settings("current"),
    )

    assert first.cohort_size == 2
    # Without the retirement the same raw is re-selected by every later run,
    # which is what blocked the daily DAG behind three archived seasons.
    assert second.cohort_size == 0
    assert control.frontier[rejected_id]["state"] == "quarantined"


def test_unretired_contract_rejection_still_fails_the_wave(tmp_path):
    control, pipeline, rejected_id, _ = _schedule_less_season_wave(tmp_path)
    # A target that raced into a lease cannot be retired, and claiming progress
    # that did not shrink the cohort would spin the recovery drain forever.
    control.quarantine_contract_rejected_target = (
        lambda target_id, *, reason: False
    )

    with pytest.raises(ParseWaveError, match="Season source contract failed"):
        pipeline.recover_unprocessed_wave(
            str(uuid.uuid4()),
            page_kinds=["season"],
            settings=_settings("current"),
        )

    assert control.frontier[rejected_id]["state"] == "fetched"


def test_schedule_less_page_without_source_identity_is_never_retired(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="season",
        canonical_url="https://fbref.com/en/comps/76/2017/2017-NASL-Stats",
        source_ids={"competition_id": "76", "season_id": "2017"},
    ))
    # Tables but no competition-history backlink: a truncated or challenged
    # capture looks exactly like this, and fresher bytes can still parse.
    html = """
    <div id="content"><h1>2017 NASL Stats</h1>
      <table id="stats_squads_standard_for">
        <thead><tr><th data-stat="team">Squad</th></tr></thead>
        <tbody><tr><td data-stat="team">New York Cosmos</td></tr></tbody>
      </table>
    </div>
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
        control,
        raw,
        generic_writer=ContractWriter(),
        typed_adapter=FakeTypedAdapter(FakeTypedWriter()),
    )

    with pytest.raises(ParseWaveError, match="Season source contract failed"):
        pipeline.recover_unprocessed_wave(
            str(uuid.uuid4()),
            page_kinds=["season"],
            settings=_settings("current"),
        )

    assert control.frontier[record.target_id]["state"] == "fetched"


def test_superseded_raw_of_a_rejecting_page_is_skipped_not_retired(tmp_path):
    control, pipeline, rejected_id, _ = _schedule_less_season_wave(tmp_path)
    # A newer fetch already replaced these bytes.  The content guard classifies
    # the observation as stale before the contract is ever consulted, so the
    # verdict is never formed against superseded evidence.
    control.frontier[rejected_id]["last_content_hash"] = "a" * 64

    result = pipeline.recover_unprocessed_wave(
        str(uuid.uuid4()),
        page_kinds=["season"],
        settings=_settings("current"),
    )

    assert result.failures == []
    assert result.contract_quarantined == 0
    assert result.stale_typed_observations_skipped == 1
    assert control.frontier[rejected_id]["state"] == "fetched"


def test_mass_contract_rejection_fails_the_wave_instead_of_shrinking_scope():
    from scrapers.fbref.pipeline import _is_mass_contract_rejection

    # A few unusable archived editions: routine, the wave carries on.
    assert not _is_mass_contract_rejection(
        WaveResult(cohort_size=25, contract_quarantined=3)
    )
    # Retirements dominate a live cohort: the source moved, not the pages.
    assert _is_mass_contract_rejection(
        WaveResult(cohort_size=25, contract_quarantined=25)
    )
    # Dominant but still small stays routine, so a short backlog can drain.
    assert not _is_mass_contract_rejection(
        WaveResult(cohort_size=5, contract_quarantined=5)
    )
    # Many retirements that do not dominate stay routine too.
    assert not _is_mass_contract_rejection(
        WaveResult(cohort_size=25, contract_quarantined=10)
    )


def test_wave_result_publishes_the_counter_the_recovery_drain_reads(tmp_path):
    # The drain reads this key off as_dict(); the DAG-side test mocks the dict,
    # so bind the name here.
    assert "contract_quarantined" in WaveResult().as_dict()


def test_non_contract_parse_failure_still_fails_the_whole_wave(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="season",
        canonical_url="https://fbref.com/en/comps/76/2017/2017-NASL-Stats",
        source_ids={"competition_id": "76", "season_id": "2017"},
    ))
    refresh, record = _commit_for_parse(raw, target, "<html></html>")
    control.frontier[record.target_id] = {
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "source_ids": dict(record.source_ids),
        "state": "fetched",
        "last_content_hash": record.content_hash,
    }
    control.fetches = [{
        "target_id": "fbref:season:76:9999",
        "page_kind": record.page_kind,
        "logical_refresh_id": refresh,
    }]
    pipeline = FBrefPipeline(control, raw, generic_writer=ContractWriter())

    with pytest.raises(ParseWaveError, match="target mismatch"):
        pipeline.parse_wave(
            str(uuid.uuid4()),
            page_kinds=["season"],
            settings=_settings("current"),
        )

    assert control.frontier[record.target_id]["state"] == "fetched"


def test_duplicate_display_label_selects_one_canonical_current_edition(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.registry["612"] = {
        "competition_id": "612",
        "canonical_url": "https://fbref.com/en/comps/612/history/x",
        "name": "Supercoppa",
        "gender": "male",
        "classification": "cup:club",
        "metadata": {"last_season": "2025"},
    }
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="competition",
        canonical_url="https://fbref.com/en/comps/612/history/x",
        source_ids={"competition_id": "612"},
    ))
    html = """
    <table id="seasons"><tbody>
      <tr><th data-stat="season"><a href="/en/comps/612/2024-2025/old">2025</a></th></tr>
      <tr><th data-stat="season"><a href="/en/comps/612/2025/current">2025</a></th></tr>
    </tbody></table>
    """
    _, record = _commit_for_parse(raw, target, html)
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    seeded, skipped = pipeline._parse_competition(
        str(uuid.uuid4()), html, record, run_type="current"
    )

    assert skipped == 0
    assert seeded == 1
    current = [entry for entry in control.seasons if entry.is_current]
    assert [entry.season_id for entry in current] == ["2025"]
    display_aliases = [
        alias for alias, _ in control.season_aliases.values()
        if alias.alias_kind == "label" and alias.alias == "2025"
    ]
    assert [alias.season_id for alias in display_aliases] == ["2025"]
    current_targets = [
        row for row in control.frontier.values()
        if row["page_kind"] == "season"
    ]
    assert len(current_targets) == 1
    assert current_targets[0]["source_ids"]["season_id"] == "2025"


def test_non_conflicting_display_label_remains_resolvable(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.registry["9"] = {
        "competition_id": "9",
        "canonical_url": "https://fbref.com/en/comps/9/history/x",
        "name": "Premier League",
        "gender": "male",
        "classification": "league:club",
        "metadata": {"last_season": "2025"},
    }
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="competition",
        canonical_url="https://fbref.com/en/comps/9/history/x",
        source_ids={"competition_id": "9"},
    ))
    html = """
    <table id="seasons"><tbody>
      <tr><th data-stat="season"><a href="/en/comps/9/2024-2025/x">2025</a></th></tr>
    </tbody></table>
    """
    _, record = _commit_for_parse(raw, target, html)
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    seeded, skipped = pipeline._parse_competition(
        str(uuid.uuid4()), html, record, run_type="current"
    )

    assert (seeded, skipped) == (1, 0)
    source_alias, _ = control.season_aliases[(
        "fbref", "9", "2024-2025"
    )]
    display_alias, _ = control.season_aliases[("fbref", "9", "2025")]
    assert source_alias.season_id == "2024-2025"
    assert source_alias.alias_kind == "source"
    assert display_alias.season_id == "2024-2025"
    assert display_alias.alias_kind == "label"


def test_source_season_ids_win_over_shifted_display_labels(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.registry["719"] = {
        "competition_id": "719",
        "canonical_url": "https://fbref.com/en/comps/719/history/x",
        "name": "FIFA Club World Cup",
        "gender": "male",
        "classification": "cup:club",
        "metadata": {"last_season": "2025"},
    }
    target = page_target_from_link(DiscoveredPageLink(
        page_kind="competition",
        canonical_url="https://fbref.com/en/comps/719/history/x",
        source_ids={"competition_id": "719"},
    ))
    html = """
    <table id="seasons"><tbody>
      <tr><th data-stat="season"><a href="/en/comps/719/2019/x">2019</a></th></tr>
      <tr><th data-stat="season"><a href="/en/comps/719/2020/x">2021</a></th></tr>
      <tr><th data-stat="season"><a href="/en/comps/719/2021/x">2022</a></th></tr>
      <tr><th data-stat="season"><a href="/en/comps/719/2022/x">2023</a></th></tr>
      <tr><th data-stat="season"><a href="/en/comps/719/2023/x">2023</a></th></tr>
      <tr><th data-stat="season"><a href="/en/comps/719/2025/x">2025</a></th></tr>
    </tbody></table>
    """
    _, record = _commit_for_parse(raw, target, html)
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    seeded, skipped = pipeline._parse_competition(
        str(uuid.uuid4()), html, record, run_type="current"
    )

    assert skipped == 0
    assert seeded == 1
    current = [entry for entry in control.seasons if entry.is_current]
    assert [entry.season_id for entry in current] == ["2025"]
    resolved = {
        alias.alias: alias.season_id
        for alias, _ in control.season_aliases.values()
    }
    assert resolved == {
        "2019": "2019",
        "2020": "2020",
        "2021": "2021",
        "2022": "2022",
        "2023": "2023",
        "2025": "2025",
    }
    alias_2021, _ = control.season_aliases[("fbref", "719", "2021")]
    alias_2022, _ = control.season_aliases[("fbref", "719", "2022")]
    assert alias_2021.alias_kind == "source"
    assert alias_2022.alias_kind == "source"
    assert not any(
        alias.alias_kind == "label" and alias.alias in {"2021", "2022"}
        for alias, _ in control.season_alias_calls
    )


def _strict_acceptance_summary(control, run_id):
    target_id = "fbref:competition_index:all"
    target_ids = [target_id]
    cohort_hash = hashlib.sha256(
        json.dumps(
            target_ids, ensure_ascii=True, separators=(",", ":")
        ).encode("ascii")
    ).hexdigest()
    raw_audit = {
        "schema_version": "fbref-raw-audit-anchor-v1",
        "status": "passed",
        "run_type": "current",
        "audited_control_run_id": run_id,
        "processing_control_run_id": run_id,
        "successful_attempt_count": 1,
        "audited_attempt_count": 1,
        "zero_delta_required": False,
    }
    cohort = {
        "schema_version": "fbref-acceptance-cohort-v1",
        "status": "frozen",
        "scope": "current",
        "cohort_size": 1,
        "cohort_sha256": cohort_hash,
        "target_ids": target_ids,
        "required_page_kinds": ["competition_index"],
        "required_routes": [],
        "coverage_slots": {"competition_index": target_id},
    }
    control.run.update(
        run_type="current",
        status="running",
        request_limit=100,
        byte_limit=50 * 1024 * 1024,
        requests_used=1,
        bytes_used=128,
        metadata={
            "execution_mode": "acceptance_nonpublishing",
            "acceptance_profile": True,
            "acceptance_scope": "current",
            "shard_size": 25,
            "publication_eligible": False,
            "bootstrap_only": False,
            "raw_audit": raw_audit,
            "acceptance_cohort": cohort,
        },
    )
    summary = control.get_run_summary(run_id)
    summary.update(
        control.run,
        target_counts={"succeeded": 1},
        dataset_validation_counts={"succeeded": 2, "skipped": 1},
        unvalidated_target_count=0,
        unprocessed_raw_count=0,
        budget_exceeded=False,
        traffic_totals={
            "network_attempts": 1,
            "warm_http_successes": 1,
            "warm_http_success_rate": 1.0,
            "unclassified_failures": 0,
            "unclassified_failure_rate": 0.0,
            "duplicate_fetch_violations": 0,
        },
        table_availability={
            "available": 1,
            "empty": 1,
            "restricted": 1,
            "not_applicable": 1,
        },
        cohort_page_kind_counts={"competition_index": 1},
        cohort_route_counts={"competition_index": 1},
        session_metrics={"max_bootstraps_per_session": 1},
    )
    control.acceptance_evidence = {
        "summary": summary,
        "targets": [
            {
                "target_id": target_id,
                "status": "succeeded",
                "page_kind": "competition_index",
                "source_ids": {"competition_index": "all"},
                "http_status": 200,
                "raw_manifest_key": "raw/manifest.json",
                "content_hash": "a" * 64,
                "evidence_class": None,
            }
        ],
        "datasets": [
            {
                "target_id": target_id,
                "dataset": "__page__",
                "availability": "empty",
                "parse_status": "succeeded",
                "persistence_status": "succeeded",
                "validation_status": "succeeded",
                "row_count": 0,
                "empty_reason": "verified_zero_table_page",
            }
        ],
    }
    return summary


def test_strict_acceptance_passes_explicit_absence_and_ignores_global_backlog(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    run_id = str(uuid.uuid4())
    summary = _strict_acceptance_summary(control, run_id)
    summary.update(
        unknown_gender_registry_count=99,
        female_downstream_targets=99,
        global_unprocessed_raw_sla_overdue_count=99,
        crawlable_frontier_scope_counts={"female_gender": 99},
        sentinel_coverage={},
    )
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    pipeline.validate_and_finish(run_id, acceptance=True)

    assert "bronze_acceptance_anchored" in control.events
    assert "finish:True" in control.events
    marker = control.run["metadata"]["bronze_acceptance"]
    assert marker["strict_gates"]["warm_http_success_rate"] == 1.0


@pytest.mark.parametrize(
    ("requests_used", "bytes_used"),
    [
        (4096, 128),
        (1, 2048 * 1024 * 1024),
    ],
)
def test_production_reaching_either_safety_circuit_fails_validation(
    tmp_path, requests_used, bytes_used
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    run_id = str(uuid.uuid4())
    summary = _strict_acceptance_summary(control, run_id)
    summary.update(
        run_type="current",
        request_limit=4096,
        byte_limit=2048 * 1024 * 1024,
        requests_used=requests_used,
        bytes_used=bytes_used,
    )
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(RunValidationError, match="production_safety_circuit_reached"):
        pipeline.validate_and_finish(run_id, publication_eligible=False)

    assert "finish:True" not in control.events


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        (
            lambda summary, evidence: summary.update(
                target_counts={"succeeded": 0, "skipped": 1}
            ),
            "cohort_targets_not_succeeded",
        ),
        (
            lambda summary, evidence: summary["traffic_totals"].update(
                warm_http_successes=0, warm_http_success_rate=0.0
            ),
            "warm_http_successes=0!=1",
        ),
        (
            lambda summary, evidence: summary["traffic_totals"].update(
                unclassified_failures=1
            ),
            "unclassified_failures=1",
        ),
        (
            lambda summary, evidence: summary["traffic_totals"].update(
                duplicate_fetch_violations=1
            ),
            "duplicate_fetch_violations=1",
        ),
        (
            lambda summary, evidence: summary.update(budget_exceeded=True),
            "budget_exceeded=true",
        ),
        (
            lambda summary, evidence: summary.update(
                table_availability={"unknown": 1}
            ),
            "unsafe_table_availability",
        ),
        (
            lambda summary, evidence: evidence["datasets"][0].update(
                empty_reason=None
            ),
            "acceptance_dataset_evidence_invalid",
        ),
    ],
)
def test_strict_acceptance_fails_closed_on_every_strict_gate(
    tmp_path, mutation, error
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    run_id = str(uuid.uuid4())
    summary = _strict_acceptance_summary(control, run_id)
    mutation(summary, control.acceptance_evidence)
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(RunValidationError, match=error):
        pipeline.validate_and_finish(run_id, acceptance=True)

    assert "bronze_acceptance_anchored" not in control.events
    assert "finish:True" not in control.events


def test_strict_acceptance_requires_generic_completion_for_each_target(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    run_id = str(uuid.uuid4())
    summary = _strict_acceptance_summary(control, run_id)
    second_target_id = "fbref:competition:9"
    target_ids = [
        "fbref:competition_index:all",
        second_target_id,
    ]
    cohort = summary["metadata"]["acceptance_cohort"]
    cohort.update(
        cohort_size=2,
        cohort_sha256=hashlib.sha256(
            json.dumps(
                target_ids, ensure_ascii=True, separators=(",", ":")
            ).encode("ascii")
        ).hexdigest(),
        target_ids=target_ids,
        required_page_kinds=["competition_index", "competition"],
        coverage_slots={
            "competition_index": target_ids[0],
            "competition": second_target_id,
        },
    )
    summary.update(
        target_counts={"succeeded": 2},
        cohort_page_kind_counts={
            "competition_index": 1,
            "competition": 1,
        },
        cohort_route_counts={"competition_index": 1, "competition": 1},
    )
    summary["metadata"]["raw_audit"].update(
        successful_attempt_count=2,
        audited_attempt_count=2,
    )
    summary["traffic_totals"].update(
        network_attempts=2,
        warm_http_successes=2,
    )
    control.acceptance_evidence["targets"].append(
        {
            "target_id": second_target_id,
            "status": "succeeded",
            "page_kind": "competition",
            "source_ids": {"competition_id": "9"},
            "http_status": 200,
            "raw_manifest_key": "raw/competition-manifest.json",
            "content_hash": "b" * 64,
            "evidence_class": None,
        }
    )
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(
        RunValidationError,
        match=f"acceptance_page_completion_missing={second_target_id}",
    ):
        pipeline.validate_and_finish(run_id, acceptance=True)

    assert "bronze_acceptance_anchored" not in control.events
    assert "finish:True" not in control.events


def test_strict_acceptance_rejects_stale_marker_without_typed_completion(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    run_id = str(uuid.uuid4())
    summary = _strict_acceptance_summary(control, run_id)
    target_id = "fbref:competition_index:all"
    cohort = summary["metadata"]["acceptance_cohort"]
    cohort.update(
        required_page_kinds=["schedule"],
        coverage_slots={"schedule": target_id},
    )
    summary.update(
        cohort_page_kind_counts={"schedule": 1},
        cohort_route_counts={"schedule": 1},
    )
    target = control.acceptance_evidence["targets"][0]
    target.update(
        page_kind="schedule",
        source_ids={"competition_id": "9", "season_id": "2025-2026"},
    )
    control.acceptance_evidence["datasets"].append(
        {
            "target_id": target_id,
            "dataset": "typed:__stale_observation__",
            "availability": "duplicate",
            "parse_status": "succeeded",
            "persistence_status": "skipped",
            "validation_status": "skipped",
            "row_count": 0,
            "empty_reason": None,
        }
    )
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(
        RunValidationError,
        match=f"acceptance_typed_completion_missing={target_id}",
    ):
        pipeline.validate_and_finish(run_id, acceptance=True)

    assert "bronze_acceptance_anchored" not in control.events
    assert "finish:True" not in control.events


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
    assert "finish:False" not in control.events


def test_validation_treats_failed_targets_as_returned_to_queue(tmp_path):
    """Mirror of the wave gate for #1102: a terminally failed target is the
    frontier's problem now, not this run's — validation must not brand the
    resumed run incomplete over it. Traffic gates still police run quality."""
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    base_summary = control.get_run_summary(str(uuid.uuid4()))
    control.get_run_summary = lambda _, **__: {
        **base_summary,
        "target_counts": {"succeeded": 1, "failed": 1},
        "traffic_totals": {"warm_http_success_rate": 1.0},
    }
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    pipeline.validate_and_finish(str(uuid.uuid4()))

    assert "finish:True" in control.events


def test_lone_validate_clear_reanimates_before_finishing_green(tmp_path):
    """Clearing only the validate task never re-runs the waves; when the
    gates pass on a 'failed' run, validation itself must reanimate before
    finish_run, or the resume dies on StateConflict (#1102)."""
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.run["status"] = "failed"
    captured = {}

    def start_run(run_id, **kwargs):
        control.events.append("start_run")
        captured.update(kwargs)
        control.run["status"] = "running"

    control.start_run = start_run
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    pipeline.validate_and_finish(str(uuid.uuid4()))

    assert "start_run" in control.events
    assert "finish:True" in control.events
    # The run is finishing right now — reopened 'retry' targets would linger
    # in a succeeded run forever.
    assert captured == {"reopen_targets": False}


def test_run_live_waves_reanimates_a_failed_run_before_the_first_wave(
    tmp_path,
):
    """``airflow tasks clear -t run_live_waves`` re-runs only this task,
    never initialize_run, so the waves themselves must reanimate the aborted
    control run (#1102)."""
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.run["status"] = "failed"

    def start_run(run_id):
        control.events.append("start_run")
        control.run["status"] = "running"

    control.start_run = start_run
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())
    pipeline.fetch_wave = lambda *a, **k: WaveResult()
    pipeline.parse_wave = lambda *a, **k: WaveResult()

    pipeline.run_live_waves(
        str(uuid.uuid4()),
        worker_id="resume-live",
        page_kinds=["competition_index"],
        settings=_settings(),
    )

    assert "start_run" in control.events


def test_persistent_aborted_run_is_rejected_before_reanimation_or_fetch(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = PersistentFakeControl(raw)
    control.run["status"] = "failed"

    def reject_inexact(_run_id):
        control.events.append("persistent_preflight")
        raise StateConflict("aborted persistent session")

    control.assert_persistent_metering_reconciled = reject_inexact
    control.start_run = lambda _run_id: (_ for _ in ()).throw(
        AssertionError("an inexact persistent run must not be reanimated")
    )
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())
    pipeline.fetch_wave = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("an inexact persistent run must not fetch")
    )

    with pytest.raises(StateConflict, match="aborted persistent session"):
        pipeline.run_live_waves(
            str(uuid.uuid4()),
            worker_id="resume-persistent",
            page_kinds=["competition_index"],
            settings=_persistent_settings(),
        )

    assert control.events[-1] == "persistent_preflight"


def test_run_live_waves_does_not_touch_a_healthy_run(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.run["status"] = "running"

    def start_run(run_id):
        raise AssertionError("healthy runs must not be restarted mid-flight")

    control.start_run = start_run
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())
    pipeline.fetch_wave = lambda *a, **k: WaveResult()
    pipeline.parse_wave = lambda *a, **k: WaveResult()

    pipeline.run_live_waves(
        str(uuid.uuid4()),
        worker_id="resume-live",
        page_kinds=["competition_index"],
        settings=_settings(),
    )


def test_validation_accepts_complete_eligible_sentinel_coverage(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    pipeline.validate_and_finish(str(uuid.uuid4()))

    assert "finish:True" in control.events


def test_canary_validation_does_not_require_global_publication_freshness(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = control.get_run_summary(str(uuid.uuid4()))
    summary["publication_scope_freshness"] = {
        "total_targets": 4,
        "fresh_targets": 0,
        "stale_targets": 4,
        "all_within_sla": False,
    }
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    pipeline.validate_and_finish(str(uuid.uuid4()), publication_eligible=False)

    assert "finish:True" in control.events


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    [
        ({"unprocessed_raw_count": 2}, "unprocessed_raw_count=2"),
        (
            {"global_unprocessed_raw_sla_overdue_count": 2},
            "global_unprocessed_raw_sla_overdue_count=2",
        ),
        (
            {"unknown_gender_registry_count": 1},
            "unknown_gender_registry_count=1",
        ),
        (
            {
                "crawlable_frontier_scope_counts": {
                    "eligible_male": 1,
                    "female_gender": 3,
                }
            },
            "crawlable_out_of_scope_targets",
        ),
        (
            {
                "current_scope_freshness": {
                    "total_targets": 4,
                    "fresh_targets": 3,
                    "stale_targets": 1,
                    "all_within_sla": False,
                }
            },
            "current_scope_stale_targets=1",
        ),
    ],
)
def test_validation_enforces_production_scope_and_recovery_gates(
    tmp_path, mutation, expected_error,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = control.get_run_summary(str(uuid.uuid4()))
    summary.update(mutation)
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(RunValidationError, match=expected_error):
        pipeline.validate_and_finish(str(uuid.uuid4()))

    assert "finish:False" not in control.events


def test_validation_allows_fresh_raw_owned_by_a_concurrent_run(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = control.get_run_summary(str(uuid.uuid4()))
    summary.update({
        "unprocessed_raw_count": 0,
        "global_unprocessed_raw_count": 3,
        "global_unprocessed_raw_sla_overdue_count": 0,
    })
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    pipeline.validate_and_finish(str(uuid.uuid4()))

    assert "finish:True" in control.events


def test_validation_rejects_a_browser_driven_page_by_page_session(tmp_path):
    """The invariant is one clearance per session, every page then riding the
    warm HTTP path: a regression that drove the browser per page shows one
    bootstrap attempt per page."""
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = control.get_run_summary(str(uuid.uuid4()))
    summary["session_metrics"] = {"max_bootstraps_per_session": 25}
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(
        RunValidationError,
        match="browser_bootstrap_exceeded_per_session",
    ):
        pipeline.validate_and_finish(str(uuid.uuid4()))

    assert "finish:False" not in control.events


def test_validation_accepts_a_clearance_re_solved_on_a_fresh_proxy(tmp_path):
    """A stalled exit IP costs a second solve, which the run reserved and the
    transport bounds. Demanding a single attempt failed a production run whose
    only sin was surviving a bad proxy."""
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = control.get_run_summary(str(uuid.uuid4()))
    summary["session_metrics"] = {"max_bootstraps_per_session": 2}
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    pipeline.validate_and_finish(str(uuid.uuid4()))

    assert "finish:True" in control.events


def test_validation_reconciles_persistent_meter_before_finishing(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = control.get_run_summary(str(uuid.uuid4()))
    summary["metadata"] = {
        **dict(summary.get("metadata") or {}),
        "persistent_http_session": True,
    }
    control.get_run_summary = lambda _, **__: summary
    control.assert_persistent_metering_reconciled = (
        lambda run_id: control.events.append(f"persistent_reconciled:{run_id}")
    )
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())
    run_id = str(uuid.uuid4())

    pipeline.validate_and_finish(run_id)

    assert control.events.index(f"persistent_reconciled:{run_id}") < (
        control.events.index("finish:True")
    )


def _ordinary_validation_summary(control, **traffic):
    summary = control.get_run_summary(str(uuid.uuid4()))
    summary.update(
        target_counts={"succeeded": 19},
        promotion_pending_match_count=0,
        traffic_totals={
            "network_attempts": 20,
            "warm_http_successes": 19,
            "warm_http_success_rate": 0.95,
            "unclassified_failures": 0,
            "unclassified_failure_rate": 0.0,
            "duplicate_fetch_violations": 0,
            **traffic,
        },
        session_metrics={"max_bootstraps_per_session": 1},
    )
    return summary


def test_productive_run_warns_on_partial_warm_http_success(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = _ordinary_validation_summary(
        control,
        warm_http_success_rate=0.9494,
    )
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    result = pipeline.validate_and_finish(str(uuid.uuid4()))

    assert result["warnings"]["warm_http_success_rate"] == pytest.approx(
        0.9494
    )
    assert "finish:True" in control.events


@pytest.mark.parametrize("success_rate", [0.5, 0.949999])
def test_productive_run_warns_at_partial_success_boundaries(
    tmp_path, success_rate
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = _ordinary_validation_summary(
        control,
        warm_http_success_rate=success_rate,
    )
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    result = pipeline.validate_and_finish(str(uuid.uuid4()))

    assert result["warnings"]["warm_http_success_rate"] == pytest.approx(
        success_rate
    )


def test_ninety_five_percent_warm_success_needs_no_warning(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = _ordinary_validation_summary(control)
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    result = pipeline.validate_and_finish(str(uuid.uuid4()))

    assert "warnings" not in result


@pytest.mark.parametrize(
    ("metric", "value"),
    [("requests_reserved", 1), ("bytes_reserved", 1)],
)
def test_clean_productive_run_hard_fails_open_reservations(
    tmp_path, metric, value
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = _ordinary_validation_summary(
        control,
        warm_http_successes=20,
        warm_http_success_rate=1.0,
    )
    summary[metric] = value
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(RunValidationError, match=rf"{metric}={value}"):
        pipeline.validate_and_finish(str(uuid.uuid4()))


def test_partial_warm_success_with_unresolved_claim_is_hard(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = _ordinary_validation_summary(
        control,
        warm_http_success_rate=0.9494,
    )
    summary["target_counts"]["leased"] = 1
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(
        RunValidationError,
        match="partial_warm_http_success_without_recoverable_progress",
    ):
        pipeline.validate_and_finish(str(uuid.uuid4()))


def test_zero_warm_success_after_attempts_is_a_hard_failure(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = _ordinary_validation_summary(
        control,
        warm_http_successes=0,
        warm_http_success_rate=0.0,
    )
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(RunValidationError, match="zero warm HTTP successes"):
        pipeline.validate_and_finish(str(uuid.uuid4()))


def test_warm_success_below_half_is_a_hard_failure(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = _ordinary_validation_summary(
        control,
        warm_http_success_rate=0.499999,
    )
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(RunValidationError, match="warm_http_success_rate"):
        pipeline.validate_and_finish(str(uuid.uuid4()))


def test_claimed_work_without_durable_progress_is_a_hard_failure(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = _ordinary_validation_summary(control)
    summary["target_counts"] = {"failed": 20}
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(RunValidationError, match="no_durable_progress"):
        pipeline.validate_and_finish(str(uuid.uuid4()))


def test_failed_cohort_without_network_still_has_no_durable_progress(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = _ordinary_validation_summary(
        control,
        network_attempts=0,
        warm_http_successes=0,
        warm_http_success_rate=None,
    )
    summary["target_counts"] = {"failed": 1}
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(RunValidationError, match="no_durable_progress"):
        pipeline.validate_and_finish(str(uuid.uuid4()))


def test_truly_empty_zero_network_run_is_valid_no_work(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = _ordinary_validation_summary(
        control,
        network_attempts=0,
        warm_http_successes=0,
        warm_http_success_rate=None,
    )
    summary["target_counts"] = {}
    summary["dataset_validation_counts"] = {}
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    result = pipeline.validate_and_finish(str(uuid.uuid4()))

    assert "finish:True" in control.events
    assert "warnings" not in result


def test_any_unclassified_failure_remains_hard(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = _ordinary_validation_summary(
        control,
        unclassified_failures=1,
        unclassified_failure_rate=0.0001,
    )
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(RunValidationError, match="unclassified_failures=1"):
        pipeline.validate_and_finish(str(uuid.uuid4()))


def test_productive_current_run_warns_on_recoverable_promotion_backlog(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = _ordinary_validation_summary(control)
    summary["promotion_pending_match_count"] = 26
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    result = pipeline.validate_and_finish(str(uuid.uuid4()))

    assert result["warnings"]["promotion_pending_match_count"] == 26
    assert "finish:True" in control.events


def test_unproductive_current_run_blocks_promotion_backlog(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = _ordinary_validation_summary(control)
    summary["target_counts"] = {"failed": 20}
    summary["promotion_pending_match_count"] = 26
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(
        RunValidationError, match="promotion_pending_match_count=26"
    ):
        pipeline.validate_and_finish(str(uuid.uuid4()))


def test_current_publication_warns_on_productive_pending_match_backlog(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = control.get_run_summary(str(uuid.uuid4()))
    summary["promotion_pending_match_count"] = 26
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    result = pipeline.validate_and_finish(str(uuid.uuid4()))

    assert result["warnings"]["promotion_pending_match_count"] == 26
    assert "finish:True" in control.events


@pytest.mark.parametrize(
    ("run_type", "publication_eligible"),
    [("current", False), ("backfill", True)],
)
def test_nonpublishing_or_noncurrent_run_reports_but_does_not_gate_global_pending(
    tmp_path, run_type, publication_eligible
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.run["run_type"] = run_type
    summary = control.get_run_summary(str(uuid.uuid4()))
    summary["promotion_pending_match_count"] = 26
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    pipeline.validate_and_finish(
        str(uuid.uuid4()),
        publication_eligible=publication_eligible,
    )

    assert "finish:True" in control.events


def test_validation_rejects_missing_sentinel_coverage(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = control.get_run_summary(str(uuid.uuid4()))
    summary["sentinel_coverage"].pop("World Cup")
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(RunValidationError, match="sentinel_coverage_missing"):
        pipeline.validate_and_finish(str(uuid.uuid4()))

    assert "finish:False" not in control.events


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

    assert "finish:False" not in control.events


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
        "fbref-discovery-parser-v5",
    )] = {"status": "succeeded"}
    control.fetches = [{
        "target_id": record.target_id,
        "page_kind": record.page_kind,
        "logical_refresh_id": refresh,
    }]
    source_run_id = str(uuid.uuid4())
    control.get_run = lambda _: _accepted_replay_source(source_run_id)
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    result = pipeline.parse_wave(
        str(uuid.uuid4()),
        source_run_id=source_run_id,
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
            "replay_source_run_not_succeeded=pending",
        ),
        (
            {"run_type": "backfill", "status": "running"},
            "replay_source_run_not_succeeded=running",
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


@pytest.mark.parametrize("status", ["failed", "cancelled"])
def test_replay_parse_rejects_unsuccessful_source_run(tmp_path, status):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.get_run = lambda _: {"run_type": "current", "status": status}
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(
        ParseWaveError, match=f"replay_source_run_not_succeeded={status}"
    ):
        pipeline.parse_wave(
            str(uuid.uuid4()),
            source_run_id=str(uuid.uuid4()),
            page_kinds=["match"],
            settings=_settings("replay"),
        )

    assert not any(event.startswith("replay:") for event in control.events)


def _accepted_replay_source(source_run_id, *, run_type="current"):
    return {
        "run_type": run_type,
        "status": "succeeded",
        "request_limit": 4096,
        "byte_limit": 2048 * 1024 * 1024,
        "metadata": {
            "execution_mode": "publishing",
            "bootstrap_only": False,
            "publication_eligible": True,
            "raw_audit": {
                "schema_version": "fbref-raw-audit-anchor-v1",
                "status": "passed",
                "run_type": run_type,
                "audited_control_run_id": source_run_id,
                "processing_control_run_id": source_run_id,
                "successful_attempt_count": 1,
                "audited_attempt_count": 1,
                "zero_delta_required": False,
            }
        },
    }


def _accepted_nonpublishing_source(source_run_id):
    target_ids = ["fbref:competition_index:all"]
    cohort_hash = hashlib.sha256(
        json.dumps(
            target_ids, ensure_ascii=True, separators=(",", ":")
        ).encode("ascii")
    ).hexdigest()
    cohort = {
        "schema_version": "fbref-acceptance-cohort-v1",
        "status": "frozen",
        "scope": "current",
        "cohort_size": 1,
        "cohort_sha256": cohort_hash,
        "target_ids": target_ids,
        "required_page_kinds": ["competition_index"],
        "required_routes": [],
        "coverage_slots": {"competition_index": target_ids[0]},
    }
    return {
        "run_type": "current",
        "status": "succeeded",
        "request_limit": 100,
        "byte_limit": 50 * 1024 * 1024,
        "metadata": {
            "execution_mode": "acceptance_nonpublishing",
            "acceptance_profile": True,
            "acceptance_scope": "current",
            "shard_size": 25,
            "bootstrap_only": False,
            "publication_eligible": False,
            "acceptance_cohort": cohort,
            "raw_audit": {
                "schema_version": "fbref-raw-audit-anchor-v1",
                "status": "passed",
                "run_type": "current",
                "audited_control_run_id": source_run_id,
                "processing_control_run_id": source_run_id,
                "successful_attempt_count": 1,
                "audited_attempt_count": 1,
                "zero_delta_required": False,
            },
            "bronze_acceptance": {
                "schema_version": "fbref-bronze-acceptance-v1",
                "status": "passed",
                "processing_control_run_id": source_run_id,
                "scope": "current",
                "cohort_size": 1,
                "cohort_sha256": cohort_hash,
                "page_kind_counts": {"competition_index": 1},
                "route_counts": {"competition_index": 1},
                "strict_gates": {"all_cohort_targets_succeeded": True},
            },
        },
    }


@pytest.mark.parametrize(
    ("mutate", "error"),
    [
        (
            lambda run: run.update(request_limit=100),
            "replay_source_run_not_production_profile",
        ),
        (
            lambda run: run["metadata"].pop("raw_audit"),
            "replay_source_raw_audit_missing",
        ),
        (
            lambda run: run["metadata"].update(
                publication_eligible=False,
                execution_mode="bootstrap_only",
                bootstrap_only=True,
            ),
            "replay_source_run_not_publication_eligible",
        ),
        (
            lambda run: run["metadata"]["raw_audit"].update(
                status="failed"
            ),
            "replay_source_raw_audit_not_accepted",
        ),
    ],
)
def test_replay_parse_rejects_unaccepted_source_evidence(
    tmp_path, mutate, error
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    source_run_id = str(uuid.uuid4())
    source_run = _accepted_replay_source(source_run_id)
    mutate(source_run)
    control.get_run = lambda _: source_run
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    with pytest.raises(ParseWaveError, match=error):
        pipeline.parse_wave(
            str(uuid.uuid4()),
            source_run_id=source_run_id,
            page_kinds=["match"],
            settings=_settings("replay"),
        )


def test_replay_accepts_legacy_audited_production_source_without_mode_metadata(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    source_run_id = str(uuid.uuid4())
    source_run = _accepted_replay_source(source_run_id)
    source_run["metadata"].pop("execution_mode")
    source_run["metadata"].pop("bootstrap_only")
    source_run["metadata"].pop("publication_eligible")
    control.get_run = lambda _: source_run
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    assert pipeline._replay_source_error(source_run_id) is None


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

    assert "finish:False" not in control.events


def test_replay_validation_accepts_audited_production_source_run(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.run["run_type"] = "replay"
    source_run_id = str(uuid.uuid4())
    control.get_run = lambda _: _accepted_replay_source(
        source_run_id, run_type="backfill"
    )
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    pipeline.validate_and_finish(
        str(uuid.uuid4()), replay_source_run_id=source_run_id
    )

    assert "finish:True" in control.events


def test_acceptance_replay_accepts_only_separate_strict_nonpublishing_source(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    source_run_id = str(uuid.uuid4())
    replay_run_id = str(uuid.uuid4())
    source = _accepted_nonpublishing_source(source_run_id)
    metrics_core = {
        "schema_version": "fbref-pipeline-run-metrics-v1",
        "control_run_id": replay_run_id,
        "schema": "acceptance_seq",
        "mode": "sequential",
        "elapsed_seconds": 1.0,
        "match_count": 1,
        "match_keys_sha256": "a" * 64,
        "statement_counts": {"execute": 1, "execute_committing": 0},
    }
    metrics = {
        **metrics_core,
        "artifact_sha256": hashlib.sha256(
            json.dumps(
                metrics_core, sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest(),
    }
    replay_summary = control.get_run_summary(replay_run_id)
    replay_summary.update(
        run_type="replay",
        status="running",
        request_limit=0,
        byte_limit=0,
        requests_used=0,
        bytes_used=0,
        target_counts={},
        dataset_validation_counts={},
        unvalidated_target_count=0,
        unprocessed_raw_count=0,
        budget_exceeded=False,
        traffic_totals={
            "network_attempts": 0,
            "warm_http_successes": 0,
            "warm_http_success_rate": None,
            "unclassified_failures": 0,
            "unclassified_failure_rate": 0.0,
            "duplicate_fetch_violations": 0,
        },
        session_metrics={"max_bootstraps_per_session": 0},
        metadata={
            "execution_mode": "acceptance_replay_nonpublishing",
            "acceptance_replay": True,
            "acceptance_replay_source_run_id": source_run_id,
            "publication_eligible": False,
            "bootstrap_only": False,
            "pipeline_run_metrics": metrics,
            "raw_audit": {
                "schema_version": "fbref-raw-audit-anchor-v1",
                "status": "passed",
                "run_type": "replay",
                "audited_control_run_id": source_run_id,
                "processing_control_run_id": replay_run_id,
                "successful_attempt_count": 1,
                "audited_attempt_count": 1,
                "zero_delta_required": True,
                "artifact_sha256": "b" * 64,
            },
        },
    )
    control.run = replay_summary
    control.get_run = lambda run_id: (
        source if str(run_id) == source_run_id else replay_summary
    )
    control.get_run_summary = lambda run_id, **__: (
        source if str(run_id) == source_run_id else replay_summary
    )
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    assert pipeline._replay_source_error(source_run_id) == (
        "replay_source_run_not_production_profile"
    )
    assert pipeline._acceptance_replay_source_error(source_run_id) is None
    pipeline.validate_and_finish(
        replay_run_id,
        replay_source_run_id=source_run_id,
        acceptance=True,
        acceptance_replay=True,
    )

    assert "bronze_acceptance_replay_anchored" in control.events
    marker = control.run["metadata"]["bronze_acceptance_replay"]
    assert marker["strict_gates"][
        "pipeline_metrics_artifact_sha256"
    ] == metrics["artifact_sha256"]
    assert "finish:True" in control.events


def test_acceptance_replay_reprocesses_already_observed_source_matches(
    tmp_path,
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
    match_id = "0701e218"
    target = PageTarget(
        source="fbref",
        page_kind="match",
        target_id=f"fbref:match:{match_id}",
        canonical_url=f"https://fbref.com/en/matches/{match_id}/x",
        source_ids={
            "competition_id": "9",
            "season_id": "2025-2026",
            "match_id": match_id,
        },
    )
    html = gzip.decompress(MATCH_FIXTURE.read_bytes()).decode("utf-8")
    refresh, record = _commit_for_parse(raw, target, html)
    control.fetches = [
        {
            "target_id": record.target_id,
            "page_kind": record.page_kind,
            "logical_refresh_id": refresh,
            "content_hash": record.content_hash,
        }
    ]
    control.observations[
        (
            refresh,
            PAGE_DOCUMENT_VERSION,
            TYPED_BRONZE_PARSER_VERSION,
            DISCOVERY_PARSER_VERSION,
        )
    ] = {"status": "succeeded"}
    source_run_id = str(uuid.uuid4())
    source = _accepted_nonpublishing_source(source_run_id)
    source["metadata"]["bronze_acceptance"]["page_kind_counts"] = {
        "match": 1
    }
    control.get_run = lambda run_id: (
        source if str(run_id) == source_run_id else control.run
    )
    generic = FakeWriter()
    typed = FakeTypedWriter()
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=generic,
        typed_adapter=FakeTypedAdapter(typed),
        fetcher_factory=lambda *_args: (_ for _ in ()).throw(
            AssertionError("acceptance replay must remain zero-network")
        ),
    )
    sequential_run_id = str(uuid.uuid4())
    batch_run_id = str(uuid.uuid4())

    pipeline.batch_persist_enabled = False
    sequential = pipeline.replay_acceptance_matches(
        sequential_run_id,
        source_run_id=source_run_id,
        settings=PipelineSettings.acceptance_replay(shard_size=25),
    )
    pipeline.batch_persist_enabled = True
    batch = pipeline.replay_acceptance_matches(
        batch_run_id,
        source_run_id=source_run_id,
        settings=PipelineSettings.acceptance_replay(shard_size=25),
    )

    assert sequential.cohort_size == sequential.parsed == 1
    assert batch.cohort_size == batch.parsed == 1
    assert control.observation_claim_calls == []
    assert control.manifests == []
    assert [item[1]["run_id"] for item in generic.pages] == [
        sequential_run_id,
        batch_run_id,
    ]
    assert [item[2]["run_id"] for item in typed.calls] == [
        sequential_run_id,
        batch_run_id,
    ]
    assert generic.batch_sizes == [1]
    assert typed.batch_sizes == [1]


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
    source_run_id = None
    if run_type == "replay":
        source_run_id = str(uuid.uuid4())
        control.get_run = lambda _: _accepted_replay_source(source_run_id)

    if latest is None:
        with pytest.raises(ParseWaveError, match="TypedPromotionDeferred"):
            pipeline.parse_wave(
                str(uuid.uuid4()),
                source_run_id=source_run_id,
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
        source_run_id=source_run_id,
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
    control.registry["9"] = {
        "competition_id": "9",
        "canonical_url": "https://fbref.com/en/comps/9/history/x",
        "name": "Premier League",
        "gender": "male",
        "classification": "league:club",
        "metadata": {},
    }
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
    control.registry["9"] = {
        "competition_id": "9",
        "canonical_url": "https://fbref.com/en/comps/9/history/x",
        "name": "Premier League",
        "gender": "male",
        "classification": "league:club",
        "metadata": {},
    }
    target = PageTarget(
        source="fbref",
        page_kind="player",
        target_id="fbref:player:1234abcd",
        canonical_url="https://fbref.com/en/players/1234abcd/Player",
        source_ids={"player_id": "1234abcd"},
    )
    html = """
    <html><head>
      <link rel="canonical"
        href="https://fbref.com/en/players/1234abcd/Player">
      <meta property="og:url"
        content="https://fbref.com/en/players/1234abcd/Player">
      <meta property="og:type" content="Athlete">
    </head><body><div id="meta"><h1>Player</h1></div><main>
      <a href="/en/matches/aaaaaaaa/wrong-context">Navigation match</a>
      <a href="/en/players/1234abcd/matchlogs/">Logs root</a>
      <a href="/en/players/1234abcd/matchlogs/2025/summary/First-Slug">Logs</a>
      <a href="/en/players/1234abcd/matchlogs/2025/summary/Second-Slug">Duplicate</a>
    </main></body></html>
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
    control.provenance.append({
        "parent_target_id": "fbref:squad:parent",
        "child_target_id": target.target_id,
        "relation": "page_link:player",
        "carried_competition_id": "9",
        "carried_season_id": "2025-2026",
        "parent_content_hash": "parent-hash",
        "parser_version": DISCOVERY_PARSER_VERSION,
        "logical_refresh_id": str(uuid.uuid4()),
        "metadata": {},
    })
    pipeline = FBrefPipeline(control, raw, generic_writer=ContractWriter())

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
    matchlog_edge = next(
        edge for edge in control.provenance
        if edge["child_target_id"] == children[0]["target_id"]
    )
    assert matchlog_edge["carried_competition_id"] == "9"
    assert matchlog_edge["carried_season_id"] == "2025-2026"


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
        assert control.events.count("scope_reconcile") == 1


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
            self.created_kwargs = None

        def migrate(self):
            self.events.append("migrate")

        def reap_expired_leases(self):
            self.events.append("reap")
            self.reaped += 1
            return 3

        def create_run(self, run_type, **kwargs):
            self.events.append("create_run")
            self.created_kwargs = kwargs

        def start_run(self, run_id):
            self.events.append("start_run")

    raw = _raw_store(tmp_path)
    control = LeaseReapingControl(raw)
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    run_id = pipeline.initialize_run(
        airflow_run_id="scheduled__2026-07-12T06:00:00+00:00",
        dag_id="dag_ingest_fbref",
        settings=_settings(),
        execution_metadata={
            "execution_mode": "bootstrap_only",
            "bootstrap_only": True,
            "publication_eligible": False,
        },
    )

    assert control.reaped == 1
    assert control.events.index("reap") < control.events.index("create_run")
    assert control.created_kwargs["metadata"]["execution_mode"] == (
        "bootstrap_only"
    )
    assert control.created_kwargs["metadata"]["bootstrap_only"] is True
    assert control.created_kwargs["metadata"]["publication_eligible"] is False
    assert uuid.UUID(run_id)


def test_acceptance_initialization_and_seed_anchor_exact_nonpublishing_profile(
    tmp_path,
):
    class AcceptanceControl(FakeControl):
        def migrate(self):
            self.events.append("migrate")

        def reap_expired_leases(self):
            return 0

        def create_run(self, run_type, **kwargs):
            self.run = {
                "run_type": run_type,
                "status": "pending",
                "request_limit": kwargs["request_limit"],
                "byte_limit": kwargs["byte_limit"],
                "requests_used": 0,
                "bytes_used": 0,
                "metadata": dict(kwargs["metadata"]),
            }

        def start_run(self, run_id):
            self.run["status"] = "running"

    raw = _raw_store(tmp_path)
    control = AcceptanceControl(raw)
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())
    settings = PipelineSettings.acceptance(scope="current")
    run_id = pipeline.initialize_acceptance_run(
        airflow_run_id="manual__acceptance",
        dag_id="dag_accept_fbref_bronze",
        settings=settings,
        execution_metadata={"candidate_sha": "abc123"},
    )
    target_ids = ["fbref:competition_index:all", "fbref:competition:9"]

    result = pipeline.seed_acceptance_cohort(
        run_id,
        target_ids,
        settings=settings,
        required_page_kinds=["competition_index", "competition"],
        coverage_slots={
            "competition_index": target_ids[0],
            "competition": target_ids[1],
        },
    )

    assert control.run["metadata"]["execution_mode"] == (
        "acceptance_nonpublishing"
    )
    assert control.run["metadata"]["publication_eligible"] is False
    assert control.run["metadata"]["acceptance_profile"] is True
    assert result["target_ids"] == target_ids
    assert result["cohort_size"] == 2
    assert len(result["cohort_sha256"]) == 64
    assert control.run["metadata"]["acceptance_cohort"]["target_ids"] == (
        target_ids
    )


class FakeClearanceRejectedFetcher:
    """A poisoned warm session that always rejects its target."""

    def __init__(
        self,
        events,
        *,
        error_class="http_status",
        http_status=403,
        browser_requests=1,
    ):
        self.events = events
        self.calls = 0
        self.error_class = error_class
        self.http_status = http_status
        self.browser_requests = browser_requests

    def __enter__(self):
        self.events.append("fetcher_enter")
        return self

    def __exit__(self, *args):
        self.events.append("fetcher_exit")

    def ensure_clearance(self):
        self.events.append("browser")
        return True

    def fetch(self, url, **kwargs):
        self.calls += 1
        self.events.append("http")
        raise FetchError(
            "FBref warm session was rejected",
            error_class=self.error_class,
            http_status=self.http_status,
            wire_bytes=200,
            browser_document_bytes=500,
            browser_asset_bytes=100,
            browser_requests=self.browser_requests,
            browser_bootstrap_attempts=1,
            target_requests=1,
            http_status_history=(
                () if self.http_status is None else (self.http_status,)
            ),
            latency_ms=100,
        )


@pytest.mark.parametrize(
    ("error_class", "http_status"),
    [
        ("http_status", 403),
        ("raw_contract_cloudflare_challenge", 200),
        ("warm_session_connection", None),
    ],
)
def test_a_rejected_clearance_is_burned_instead_of_failing_every_target(
    tmp_path, error_class, http_status
):
    """Cloudflare can stop honouring a clearance mid-wave (its exit IP falls out
    of favour). Reusing the dead session 403s every remaining target — one bad
    exit IP burned a whole production wave. The wave must re-solve on a fresh
    proxy instead."""
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.run.update(request_limit=100, byte_limit=50 * 1024 * 1024)
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
    retry_one = TargetLease(
        **{
            **first.__dict__,
            "attempt_id": str(uuid.UUID(int=61)),
            "claim_token": str(uuid.UUID(int=62)),
            "lease_epoch": 2,
            "attempt_number": 2,
        }
    )
    retry_two = TargetLease(
        **{
            **first.__dict__,
            "attempt_id": str(uuid.UUID(int=63)),
            "claim_token": str(uuid.UUID(int=64)),
            "lease_epoch": 3,
            "attempt_number": 3,
        }
    )
    claims = iter(([first, second], [retry_one], [retry_two]))
    control.claim_targets = lambda *args, **kwargs: next(claims)
    factories = 0

    def factory(*_):
        nonlocal factories
        factories += 1
        if factories <= 2:
            return FakeClearanceRejectedFetcher(
                control.events,
                error_class=error_class,
                http_status=http_status,
            )
        return FakeFetcher(control.events, b"<html>ok</html>")

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=factory,
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        run_id,
        worker_id="worker-1",
        page_kinds=["competition"],
        settings=PipelineSettings(
            run_type="current",
            request_limit=100,
            byte_limit=50 * 1024 * 1024,
            shard_size=4,
            request_reservation_bytes=4 * 1024 * 1024,
            domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        ),
    )

    # The exact first logical refresh is retried twice before the untouched
    # second target; neither page is falsely failed by the dead sessions.
    assert control.events.count("session_open") == 3
    assert control.events.count("fetcher_exit") == 3
    assert result.failures == []
    assert result.requeued_dead_clearance == 2
    assert result.fetched == 2
    assert [call[0].logical_refresh_id for call in control.failed] == [
        first.logical_refresh_id,
        first.logical_refresh_id,
    ]
    assert all(call[1]["session_retry"] for call in control.failed)


def test_hard_transport_policy_stops_wave_without_new_session_or_lease(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    run_id = str(uuid.UUID(int=1))

    def lease(number):
        return TargetLease(
            attempt_id=str(uuid.UUID(int=30 + number)),
            run_id=run_id,
            target_id=f"fbref:competition:{number}",
            logical_refresh_id=str(uuid.UUID(int=40 + number)),
            canonical_url=(
                f"https://fbref.com/en/comps/{number}/history/x-Seasons"
            ),
            page_kind="competition",
            source_ids={"competition_id": str(number)},
            claim_token=str(uuid.UUID(int=50 + number)),
            lease_epoch=1,
            attempt_number=1,
            leased_by="worker-1",
            lease_expires_at=NOW + timedelta(minutes=10),
        )

    first, untouched = lease(9), lease(12)
    claim_calls = []

    def claim(*args, **kwargs):
        claim_calls.append((args, kwargs))
        return [first, untouched]

    control.claim_targets = claim
    factories = []

    def factory(*_):
        factories.append(True)
        return FakeClearanceRejectedFetcher(
            control.events,
            error_class="hard_transport_policy",
            http_status=None,
        )

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=factory,
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    with pytest.raises(FetchWaveError, match="hard_transport_policy"):
        pipeline.fetch_wave(
            run_id,
            worker_id="worker-1",
            page_kinds=["competition"],
            settings=_settings(),
        )

    assert len(claim_calls) == 1
    assert len(factories) == 1
    assert control.events.count("fetcher_enter") == 1
    assert control.events.count("fetcher_exit") == 1
    assert "session_retry" not in control.events
    assert f"requeue:{untouched.target_id}" in control.events
    assert [item[0].target_id for item in control.failed] == [first.target_id]
    assert control.failed[0][1]["error_class"] == "hard_transport_policy"


def test_unknown_paid_counter_stops_wave_and_requeues_untouched_target(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    run_id = str(uuid.UUID(int=1))

    def lease(number):
        return TargetLease(
            attempt_id=str(uuid.UUID(int=70 + number)),
            run_id=run_id,
            target_id=f"fbref:competition:{number}",
            logical_refresh_id=str(uuid.UUID(int=80 + number)),
            canonical_url=(
                f"https://fbref.com/en/comps/{number}/history/x-Seasons"
            ),
            page_kind="competition",
            source_ids={"competition_id": str(number)},
            claim_token=str(uuid.UUID(int=90 + number)),
            lease_epoch=1,
            attempt_number=1,
            leased_by="worker-1",
            lease_expires_at=NOW + timedelta(minutes=10),
        )

    current, untouched = lease(9), lease(12)
    control.claim_targets = lambda *args, **kwargs: [current, untouched]

    class CounterUncertainFetcher:
        def __init__(self):
            self.fetch_calls = 0
            self.reset_calls = 0

        def __enter__(self):
            control.events.append("fetcher_enter")
            return self

        def __exit__(self, *args):
            control.events.append("fetcher_exit")

        def ensure_clearance(self):
            control.events.append("browser")
            return True

        def fetch(self, *args, **kwargs):
            self.fetch_calls += 1
            control.events.append("http")
            raise FetchError(
                "FBref paid transport accounting is uncertain; "
                "counter=drain unavailable; final_close=close unavailable; "
                "target_error=http_status: target status=503",
                error_class="hard_transport_policy",
                http_status=503,
                wire_bytes=303,
                browser_document_bytes=500,
                browser_asset_bytes=100,
                browser_requests=3,
                browser_bootstrap_attempts=2,
                browser_unobserved_bytes=400,
                provider_billed_bytes=None,
                target_requests=2,
                http_status_history=(500, 503),
                latency_ms=321,
            )

        def reset_clearance(self):
            self.reset_calls += 1

    fetcher = CounterUncertainFetcher()
    factory_calls = []

    def factory(*args):
        factory_calls.append(args)
        return fetcher

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=factory,
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    with pytest.raises(FetchWaveError, match="hard_transport_policy"):
        pipeline.fetch_wave(
            run_id,
            worker_id="worker-1",
            page_kinds=["competition"],
            settings=_settings(),
        )

    assert len(factory_calls) == 1
    assert fetcher.fetch_calls == 1
    assert fetcher.reset_calls == 0
    assert control.events.count("session_open") == 1
    assert "session_retry" not in control.events
    assert control.settlements[0][1] == {
        "requests_used": 5,
        "bytes_used": 1303,
    }
    assert control.session_metrics[0][1]["provider_billed_bytes"] is None
    assert control.session_metrics[0][1]["http_requests"] == 2
    assert [item[0].target_id for item in control.failed] == [
        current.target_id
    ]
    failure = control.failed[0][1]
    assert failure["error_class"] == "hard_transport_policy"
    assert failure["provider_billed_bytes"] is None
    assert failure["wire_bytes"] == 303
    assert "drain unavailable" in failure["error_message"]
    assert "close unavailable" in failure["error_message"]
    assert "target status=503" in failure["error_message"]
    assert f"requeue:{untouched.target_id}" in control.events


@pytest.mark.parametrize(
    ("error_class", "http_status"),
    [
        ("http_status", 403),
        ("raw_contract_cloudflare_challenge", 200),
        ("warm_session_connection", None),
        ("warm_session_timeout", None),
    ],
)
def test_session_poison_is_classified_for_retry(error_class, http_status):
    error = FetchError(
        "poisoned warm session",
        error_class=error_class,
        http_status=http_status,
    )

    assert _session_failure(error) is True


def test_unknown_http_exception_remains_target_scoped():
    error = FetchError("decoder exploded", error_class="http_exception")

    assert _session_failure(error) is False


def test_hard_transport_policy_is_never_a_refreshable_session_failure():
    error = FetchError("hard stop", error_class="hard_transport_policy")

    assert _session_failure(error) is False


def test_live_runner_reuses_one_fetch_session_and_parses_after_each_raw_batch(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())
    events = []
    sessions = []
    fetch_results = iter((WaveResult(claimed=1, fetched=1), WaveResult()))
    parse_results = iter((WaveResult(cohort_size=1, parsed=1), WaveResult()))

    def fake_fetch(*args, _live_session, **kwargs):
        sessions.append(_live_session)
        events.append("fetch")
        _live_session.fetcher = object()
        _live_session.needs_clearance = False
        return next(fetch_results)

    def fake_parse(*args, **kwargs):
        events.append("parse")
        return next(parse_results)

    pipeline.fetch_wave = fake_fetch
    pipeline.parse_wave = fake_parse

    result = pipeline.run_live_waves(
        str(uuid.uuid4()),
        worker_id="current-live",
        page_kinds=["competition_index"],
        settings=_settings(),
        max_batches=80,
    )

    assert events == ["fetch", "parse", "fetch", "parse"]
    assert sessions[0] is sessions[1]
    assert result.batches == 2
    assert result.frontier_closed is True
    assert result.fetch.fetched == 1
    assert result.parse.parsed == 1


def test_live_runner_closes_near_deadline_session_before_offline_parse(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = PersistentFakeControl(raw)
    control.run["bytes_used"] = 120
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())

    class NearDeadlineFetcher:
        persistent_http_session = True

        def persistent_session_rollover_due(self, *, within_seconds=0):
            control.events.append(("deadline_probe", within_seconds))
            return within_seconds >= 600

        def finalize_metered_session(self):
            control.events.append("provider_finalize")
            return PersistentMeteredSessionReceipt(
                session_id="session-near-deadline",
                meter="proxy_filter_provider_path_v2",
                baseline_provider_bytes=0,
                page_provider_bytes=120,
                authoritative_provider_bytes=130,
                tail_provider_bytes=10,
            )

        def reset_clearance(self):
            control.events.append("rollover_reset")

    def fake_fetch(*_args, _live_session, **_kwargs):
        _live_session.fetcher = NearDeadlineFetcher()
        _live_session.session_id = "session-near-deadline"
        _live_session.needs_clearance = False
        _live_session.state = "active"
        _live_session.tail_reserved = True
        return WaveResult(claimed=1, fetched=1, budget_exhausted=True)

    def fake_parse(*_args, **_kwargs):
        control.events.append("offline_parse")
        return WaveResult(cohort_size=1, parsed=1)

    pipeline.fetch_wave = fake_fetch
    pipeline.parse_wave = fake_parse

    pipeline.run_live_waves(
        str(uuid.UUID(int=1)),
        worker_id="current-live",
        page_kinds=["competition_index"],
        settings=_persistent_settings(),
        max_batches=1,
    )

    assert ("deadline_probe", 600) in control.events
    assert control.events.index("provider_finalize") < control.events.index(
        "offline_parse"
    )
    assert control.events.index("session_close") < control.events.index(
        "offline_parse"
    )


@pytest.mark.parametrize("invalid", [False, True, 0, 81, 1.5, "1.5"])
def test_live_runner_accepts_only_strict_one_to_eighty_batches(
    tmp_path, invalid
):
    pipeline = FBrefPipeline(
        FakeControl(_raw_store(tmp_path)),
        _raw_store(tmp_path),
        generic_writer=FakeWriter(),
    )
    side_effects = []
    pipeline.control.get_run = lambda *_args, **_kwargs: side_effects.append(
        "get_run"
    )

    with pytest.raises(
        ValueError,
        match=r"max_batches must be (?:an integer|between 1 and 80)",
    ):
        pipeline.run_live_waves(
            str(uuid.uuid4()),
            worker_id="current-live",
            page_kinds=["match"],
            settings=_settings(),
            max_batches=invalid,
        )
    assert side_effects == []


@pytest.mark.parametrize("max_batches", [1, 80, "1", "80"])
def test_live_runner_accepts_int_and_templated_int_string(
    tmp_path, max_batches
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())
    pipeline.fetch_wave = lambda *_args, **_kwargs: WaveResult()
    pipeline.parse_wave = lambda *_args, **_kwargs: WaveResult()

    result = pipeline.run_live_waves(
        str(uuid.uuid4()),
        worker_id="current-live",
        page_kinds=["match"],
        settings=_settings(),
        max_batches=max_batches,
    )

    assert result.batches == 1


def test_productive_refreshes_do_not_accumulate_into_session_exhaustion(
    tmp_path,
):
    """A fresh session that serves a valid page is healthy. Its later expiry
    must not count together with earlier, independently productive sessions."""
    raw = _raw_store(tmp_path)
    control = BudgetAwareFakeControl(raw)
    control.run.update(
        request_limit=200,
        byte_limit=100 * 1024 * 1024,
    )
    run_id = str(uuid.UUID(int=1))

    def make_lease(number, epoch=1):
        return TargetLease(
            attempt_id=str(uuid.UUID(int=2000 + number * 10 + epoch)),
            run_id=run_id,
            target_id=f"fbref:competition:{number}",
            logical_refresh_id=str(uuid.UUID(int=3000 + number)),
            canonical_url=(
                f"https://fbref.com/en/comps/{number}/history/x-Seasons"
            ),
            page_kind="competition",
            source_ids={"competition_id": str(number)},
            claim_token=str(uuid.UUID(int=4000 + number * 10 + epoch)),
            lease_epoch=epoch,
            attempt_number=epoch,
            leased_by="worker-1",
            lease_expires_at=NOW + timedelta(minutes=10),
        )

    initial = [make_lease(number) for number in (9, 12, 13, 14, 15)]
    retries = [make_lease(number, 2) for number in (12, 13, 14)]
    claims = iter((initial, *([retry] for retry in retries)))
    control.claim_targets = lambda *args, **kwargs: next(claims)

    class ProductiveThenRejectedFetcher:
        def __init__(self):
            self.session_number = 1
            self.session_fetches = 0
            self.reset_calls = 0

        def __enter__(self):
            control.events.append("fetcher_enter")
            return self

        def __exit__(self, *args):
            control.events.append("fetcher_exit")

        def ensure_clearance(self):
            control.events.append("browser")
            return True

        def reset_clearance(self):
            self.reset_calls += 1
            self.session_number += 1
            self.session_fetches = 0

        def fetch(self, url, **kwargs):
            self.session_fetches += 1
            control.events.append("http")
            if self.session_number <= 3 and self.session_fetches == 2:
                raise FetchError(
                    "FBref warm session was rejected after a valid page",
                    error_class="http_status",
                    http_status=403,
                    wire_bytes=200,
                    target_requests=1,
                    http_status_history=(403,),
                    latency_ms=100,
                )
            body = b"<html>ok</html>"
            browser_requests = 20 if self.session_fetches == 1 else 0
            return FetchResponse(
                url=url,
                status_code=200,
                body=body,
                headers={"etag": '"v1"'},
                latency_ms=10,
                http_wire_bytes=len(body) + 120,
                decoded_html_bytes=len(body),
                http_requests=1,
                http_status_history=(200,),
                browser_document_bytes=(500 if browser_requests else 0),
                browser_asset_bytes=(100 if browser_requests else 0),
                browser_requests=browser_requests,
                browser_bootstrap_attempts=(1 if browser_requests else 0),
            )

    fetcher = ProductiveThenRejectedFetcher()
    factory_calls = []

    def factory(*args):
        factory_calls.append(args)
        return fetcher

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=factory,
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        run_id,
        worker_id="worker-1",
        page_kinds=["competition"],
        settings=PipelineSettings(
            run_type="current",
            request_limit=200,
            byte_limit=100 * 1024 * 1024,
            shard_size=5,
            request_reservation_bytes=4 * 1024 * 1024,
            domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        ),
    )

    assert len(factory_calls) == 1
    assert fetcher.session_number == 4
    assert fetcher.reset_calls == 3
    assert control.events.count("session_open") == 4
    assert [item[1]["requests"] for item in control.reservations] == [
        82,
        2,
        82,
        2,
        82,
        2,
        82,
        2,
    ]
    assert control.run["requests_used"] == 88
    assert control.run["requests_reserved"] == 0
    assert result.requests == 88
    assert result.fetched == 5
    assert result.requeued_dead_clearance == 3
    assert result.requeued_session_exhaustion == 0
    assert result.budget_exhausted is False
    assert result.failures == []
    assert len(control.failed) == 3
    assert all(item[1]["session_retry"] for item in control.failed)


def test_parse_wave_holds_publication_fence_through_external_writes(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())
    events = []
    guarded = False

    @contextmanager
    def guard(run_id, *, source="fbref"):
        nonlocal guarded
        assert source == "fbref"
        events.append(f"guard:{run_id}:enter")
        guarded = True
        try:
            yield {"owner_run_id": run_id, "active": True}
        finally:
            guarded = False
            events.append(f"guard:{run_id}:exit")

    def persist(run_id, **_kwargs):
        assert guarded is True
        events.append(f"persist:{run_id}")
        return WaveResult(parsed=1)

    control.guard_publication_lock = guard
    pipeline._parse_wave_under_publication_guard = persist
    run_id = str(uuid.uuid4())

    result = pipeline.parse_wave(
        run_id,
        page_kinds=["match"],
        settings=_settings(),
    )

    assert result.parsed == 1
    assert events == [
        f"guard:{run_id}:enter",
        f"persist:{run_id}",
        f"guard:{run_id}:exit",
    ]


def test_1501_pages_roll_over_into_multiple_exact_persistent_sessions():
    events = []

    class Fetcher:
        persistent_http_session = True

        def __init__(self):
            self.now = 0.0
            self.total = 0
            self.baseline = 0
            self.pages = 0
            self.deadline = 0.0
            self.session_id = None

        def begin_metered_session(self, session_id):
            self.session_id = session_id
            self.baseline = self.total
            self.pages = 0
            self.deadline = self.now + PERSISTENT_SESSION_MAX_AGE_SECONDS
            events.append(("begin", session_id))
            return self.baseline

        def persistent_session_rollover_due(self, *, within_seconds=0):
            assert within_seconds >= 0
            return self.now >= self.deadline

        def record_page(self):
            self.total += 10
            self.pages += 1
            self.now += 6.1

        def finalize_metered_session(self):
            self.total += 1
            return PersistentMeteredSessionReceipt(
                session_id=self.session_id,
                meter="proxy_filter_provider_path_v2",
                baseline_provider_bytes=self.baseline,
                page_provider_bytes=self.pages * 10,
                authoritative_provider_bytes=self.pages * 10 + 1,
                tail_provider_bytes=1,
            )

        def reset_clearance(self):
            events.append(("reset", self.session_id))

    class Control:
        def __init__(self):
            self.receipts = []

        def reserve_clearance_session_tail(self, *_args, **_kwargs):
            return None

        def settle_clearance_session_tail(self, session_id, receipt):
            assert receipt.session_id == session_id
            self.receipts.append(receipt)
            return {"terminal": False}

        def close_clearance_session(self, session_id, *, status):
            events.append(("close", session_id, status))

    fetcher = Fetcher()
    control = Control()
    live = _LiveFetchSession(fetcher=fetcher, persistent_enabled=True)

    for page in range(1501):
        if page == 0 or live.rollover_if_due(control):
            session_id = f"session-{len(control.receipts) + 1}"
            live.attach_control_session(session_id)
            live.begin_persistent(control, run_id="run-1", tail_bytes=100)
        fetcher.record_page()
    live.finalize(control, status="closed")

    assert len(control.receipts) == 2
    assert sum(item.page_provider_bytes for item in control.receipts) == 15010
    assert sum(item.tail_provider_bytes for item in control.receipts) == 2
    first_close = events.index(("close", "session-1", "closed"))
    second_begin = events.index(("begin", "session-2"))
    assert first_close < second_begin


def test_session_refresh_exhaustion_requeues_untouched_without_false_failures(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = BudgetAwareFakeControl(raw)
    control.run.update(
        request_limit=120,
        requests_used=35,
        byte_limit=100 * 1024 * 1024,
    )
    run_id = str(uuid.UUID(int=1))

    def make_lease(attempt, target, refresh, epoch):
        return TargetLease(
            attempt_id=str(uuid.UUID(int=attempt)),
            run_id=run_id,
            target_id=target,
            logical_refresh_id=str(uuid.UUID(int=refresh)),
            canonical_url="https://fbref.com/en/comps/9/history/x-Seasons",
            page_kind="competition",
            source_ids={"competition_id": "9"},
            claim_token=str(uuid.UUID(int=attempt + 100)),
            lease_epoch=epoch,
            attempt_number=epoch,
            leased_by="worker-1",
            lease_expires_at=NOW + timedelta(minutes=10),
        )

    first = make_lease(71, "fbref:competition:9", 81, 1)
    untouched = make_lease(72, "fbref:competition:12", 82, 1)
    retry_one = make_lease(73, first.target_id, 81, 2)
    retry_two = make_lease(74, first.target_id, 81, 3)
    claims = iter(([first, untouched], [retry_one], [retry_two]))
    control.claim_targets = lambda *args, **kwargs: next(claims)
    factories = 0

    def factory(*_):
        nonlocal factories
        factories += 1
        return FakeClearanceRejectedFetcher(
            control.events,
            browser_requests=20,
        )

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=factory,
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    with pytest.raises(
        FetchWaveError,
        match="clearance_session_refreshes_exhausted=2",
    ):
        pipeline.fetch_wave(
            run_id,
            worker_id="worker-1",
            page_kinds=["competition"],
            settings=PipelineSettings(
                run_type="current",
                request_limit=120,
                byte_limit=100 * 1024 * 1024,
                shard_size=4,
                request_reservation_bytes=4 * 1024 * 1024,
                domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
            ),
        )

    assert factories == 3
    assert control.run["requests_used"] == 98
    assert control.run["requests_reserved"] == 0
    assert len(control.reservations) == 3
    assert len(control.failed) == 3
    assert all(
        lease.logical_refresh_id == first.logical_refresh_id
        for lease, _ in control.failed
    )
    assert control.events.count(f"requeue:{untouched.target_id}") == 1
    assert not any(
        lease.target_id == untouched.target_id
        for lease, _ in control.failed
    )


@pytest.mark.parametrize("request_limit", [100, 119])
def test_dead_clearance_at_budget_boundary_stops_cleanly(
    tmp_path, request_limit
):
    """The live canary had 50 good pages, then a third 403 with only two
    requests left. That is a clean budget boundary, not permission to open a
    fourth browser and not a false refresh-exhaustion failure."""

    raw = _raw_store(tmp_path)
    control = BudgetAwareFakeControl(raw)
    control.run.update(
        request_limit=request_limit,
        requests_used=35,
        byte_limit=100 * 1024 * 1024,
    )
    run_id = str(uuid.UUID(int=1))

    def make_lease(attempt, target, refresh, epoch):
        return TargetLease(
            attempt_id=str(uuid.UUID(int=attempt)),
            run_id=run_id,
            target_id=target,
            logical_refresh_id=str(uuid.UUID(int=refresh)),
            canonical_url="https://fbref.com/en/comps/9/history/x-Seasons",
            page_kind="competition",
            source_ids={"competition_id": "9"},
            claim_token=str(uuid.UUID(int=attempt + 100)),
            lease_epoch=epoch,
            attempt_number=epoch,
            leased_by="worker-1",
            lease_expires_at=NOW + timedelta(minutes=10),
        )

    first = make_lease(171, "fbref:competition:9", 181, 1)
    untouched = make_lease(172, "fbref:competition:12", 182, 1)
    retry_one = make_lease(173, first.target_id, 181, 2)
    retry_two = make_lease(174, first.target_id, 181, 3)
    claims = iter(([first, untouched], [retry_one], [retry_two]))
    control.claim_targets = lambda *args, **kwargs: next(claims)
    factories = 0

    def factory(*_):
        nonlocal factories
        factories += 1
        return FakeClearanceRejectedFetcher(
            control.events,
            browser_requests=20,
        )

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=factory,
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        run_id,
        worker_id="worker-1",
        page_kinds=["competition"],
        settings=PipelineSettings(
            run_type="current",
            request_limit=request_limit,
            byte_limit=100 * 1024 * 1024,
            shard_size=4,
            request_reservation_bytes=4 * 1024 * 1024,
            domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        ),
    )

    assert factories == 3
    assert control.run["requests_used"] == 98
    assert control.run["requests_reserved"] == 0
    assert len(control.reservations) == 3
    assert result.failures == []
    assert result.budget_exhausted is True
    assert result.requeued_at_budget == 2
    assert result.requeued_session_exhaustion == 0
    assert result.requeued_dead_clearance == 3
    assert len(control.failed) == 3
    assert control.failed[-1][1]["requeue"] is True
    assert all(
        lease.logical_refresh_id == first.logical_refresh_id
        for lease, _ in control.failed
    )
    assert control.events.count(f"requeue:{untouched.target_id}") == 1


def test_a_late_re_solve_reserves_the_rotations_it_can_still_afford():
    """A daily run reserves four proxy rotations for a clearance, but a solve
    measures ~19 requests. Demanding all four before a mid-run re-solve ended
    every run with ~40 % of its request budget unspent (#1129)."""

    daily = PipelineSettings(
        run_type="current",
        request_limit=200,
        byte_limit=100 * 1024 * 1024,
        shard_size=25,
    )
    assert daily.bootstrap_request_reservation == 80

    plenty = 90 * 1024 * 1024
    assert affordable_clearance_reservation(
        daily, request_remaining=200, byte_remaining=plenty
    ) == (80, 16 * 1024 * 1024)
    # 78 left: the old guard needed 82 and stopped the run right here.
    assert affordable_clearance_reservation(
        daily, request_remaining=78, byte_remaining=plenty
    ) == (60, 12 * 1024 * 1024)
    assert affordable_clearance_reservation(
        daily, request_remaining=25, byte_remaining=plenty
    ) == (20, 4 * 1024 * 1024)
    # One solve plus one target is the floor; below it the ceiling is real.
    assert (
        affordable_clearance_reservation(
            daily, request_remaining=21, byte_remaining=plenty
        )
        is None
    )
    # Bytes bind the same way.
    assert affordable_clearance_reservation(
        daily, request_remaining=200, byte_remaining=8 * 1024 * 1024
    ) == (20, 4 * 1024 * 1024)
    assert (
        affordable_clearance_reservation(
            daily, request_remaining=200, byte_remaining=6 * 1024 * 1024
        )
        is None
    )

    # A run that only ever reserved one solve keeps its old boundary exactly.
    small = PipelineSettings(
        run_type="current",
        request_limit=120,
        byte_limit=100 * 1024 * 1024,
        shard_size=4,
    )
    assert small.bootstrap_request_reservation == 20
    assert affordable_clearance_reservation(
        small, request_remaining=22, byte_remaining=plenty
    ) == (20, 4 * 1024 * 1024)
    assert (
        affordable_clearance_reservation(
            small, request_remaining=21, byte_remaining=plenty
        )
        is None
    )


def test_a_warm_session_failure_late_in_a_run_re_solves_on_a_smaller_budget(
    tmp_path,
):
    """The run had 78 requests left and a dead warm session. It could pay for
    three rotations, but the guard asked for four and handed the rest of the
    shard back — 2-10 times a day, on the drain runner too (#1129)."""

    raw = _raw_store(tmp_path)
    control = BudgetAwareFakeControl(raw)
    control.run.update(
        request_limit=200,
        requests_used=40,
        byte_limit=100 * 1024 * 1024,
    )
    run_id = str(uuid.UUID(int=1))

    def make_lease(attempt, target, refresh, epoch):
        return TargetLease(
            attempt_id=str(uuid.UUID(int=attempt)),
            run_id=run_id,
            target_id=target,
            logical_refresh_id=str(uuid.UUID(int=refresh)),
            canonical_url="https://fbref.com/en/comps/",
            page_kind="competition_index",
            source_ids={"competition_index": "all"},
            claim_token=str(uuid.UUID(int=attempt + 100)),
            lease_epoch=epoch,
            attempt_number=epoch,
            leased_by="worker-1",
            lease_expires_at=NOW + timedelta(minutes=10),
        )

    warm_up = make_lease(191, "fbref:competition_index:all", 191, 1)
    rejected = make_lease(192, "fbref:competition_index:all", 192, 1)
    retry = make_lease(193, rejected.target_id, 192, 2)
    claims = iter(([warm_up, rejected], [retry]))
    control.claim_targets = lambda *args, **kwargs: next(claims)

    class LateFailureFetcher:
        """The clearance spends its whole allowance, then the session dies."""

        def __init__(self, events):
            self.events = events
            self.calls = 0

        def __enter__(self):
            self.events.append("fetcher_enter")
            return self

        def __exit__(self, *args):
            self.events.append("fetcher_exit")

        def ensure_clearance(self):
            self.events.append("browser")
            return True

        def fetch(self, url, **kwargs):
            self.calls += 1
            self.events.append("http")
            if self.calls == 2:
                raise FetchError(
                    "FBref warm session lost its connection",
                    error_class="warm_session_connection",
                    http_status=None,
                    wire_bytes=200,
                    browser_requests=0,
                    target_requests=1,
                    http_requests=1,
                    http_status_history=(),
                    latency_ms=100,
                )
            body = b"<html>ok</html>"
            http_requests = 78 if self.calls == 1 else 1
            return FetchResponse(
                url=url,
                status_code=200,
                body=body,
                headers={"etag": '"v1"'},
                latency_ms=10,
                http_wire_bytes=len(body) + 120,
                decoded_html_bytes=len(body),
                http_requests=http_requests,
                http_status_history=(200,) * http_requests,
                browser_document_bytes=500,
                browser_asset_bytes=100,
                browser_requests=1,
                browser_bootstrap_attempts=1,
            )

    allowances = []

    def factory(proxy_file, max_browser_requests, max_browser_bytes):
        allowances.append((max_browser_requests, max_browser_bytes))
        return LateFailureFetcher(control.events)

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=factory,
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        run_id,
        worker_id="worker-1",
        page_kinds=["competition_index"],
        settings=PipelineSettings(
            run_type="current",
            request_limit=200,
            byte_limit=100 * 1024 * 1024,
            shard_size=4,
            domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        ),
    )

    # The wave finished its shard instead of returning it at a false ceiling.
    assert result.budget_exhausted is False
    assert result.requeued_at_budget == 0
    assert result.requeued_dead_clearance == 1
    assert result.failures == []
    # First solve got the full reservation, the re-solve got what still fit,
    # and the transport was rebuilt so it cannot outspend the smaller booking.
    assert allowances == [(80, 16 * 1024 * 1024), (60, 12 * 1024 * 1024)]
    assert control.reservations[-1][1]["requests"] == 62
    assert control.run["requests_used"] <= 200


def test_canary_that_hits_its_budget_requeues_and_ends_clean(tmp_path):
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
        fetcher_factory=lambda *_: FakeFetcher(control.events, b"<html>ok</html>"),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        run_id,
        worker_id="worker-1",
        page_kinds=["competition"],
        settings=PipelineSettings(
            run_type="current",
            request_limit=100,
            byte_limit=50 * 1024 * 1024,
            shard_size=4,
            domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        ),
    )

    assert result.failures == []
    assert result.budget_exhausted is True
    assert result.requeued_at_budget == 2
    assert [item.target_id for item in requeued] == [
        "fbref:competition:9",
        "fbref:competition:12",
    ]


def test_production_safety_circuit_exhaustion_is_loud_after_requeue(tmp_path):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    run_id = str(uuid.UUID(int=1))
    requeued = []
    lease = TargetLease(
        attempt_id=str(uuid.UUID(int=71)),
        run_id=run_id,
        target_id="fbref:competition:9",
        logical_refresh_id=str(uuid.UUID(int=72)),
        canonical_url="https://fbref.com/en/comps/9/history/x-Seasons",
        page_kind="competition",
        source_ids={"competition_id": "9"},
        claim_token=str(uuid.UUID(int=73)),
        lease_epoch=1,
        attempt_number=1,
        leased_by="worker-1",
        lease_expires_at=NOW + timedelta(minutes=10),
    )
    control.claim_targets = lambda *args, **kwargs: [lease]
    control.reserve_budget = lambda *args, **kwargs: (_ for _ in ()).throw(
        BudgetExceeded("request safety circuit exhausted")
    )
    control.requeue_unfetched_targets = lambda items: (
        requeued.extend(items) or len(items)
    )
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=lambda *_: FakeFetcher(control.events, b"<html>ok</html>"),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    with pytest.raises(FetchWaveError, match="production_safety_circuit_exhausted"):
        pipeline.fetch_wave(
            run_id,
            worker_id="worker-1",
            page_kinds=["competition"],
            settings=PipelineSettings(
                run_type="current",
                request_limit=4096,
                byte_limit=2048 * 1024 * 1024,
                shard_size=25,
                domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
            ),
        )

    assert [item.target_id for item in requeued] == [lease.target_id]


def test_the_browser_may_only_spend_the_requests_the_run_reserved(tmp_path):
    """The bootstrap reservation is what the browser is allowed to spend, so it
    also decides how many stalled exit IPs a wave can survive. Handing the
    transport a cap the run never reserved is how a wave overspent its ceiling."""
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    captured = {}

    def factory(
        proxy_file,
        max_browser_requests,
        max_browser_bytes,
    ):
        captured["proxy_file"] = proxy_file
        captured["max_browser_requests"] = max_browser_requests
        captured["max_browser_bytes"] = max_browser_bytes
        return FakeFetcher(control.events, b"<html>ok</html>")

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=factory,
        sleep=lambda _: None,
        clock=lambda: NOW,
    )
    settings = PipelineSettings(
        run_type="current",
        request_limit=200,
        byte_limit=100 * 1024 * 1024,
        shard_size=4,
        request_reservation_bytes=7 * 1024 * 1024,
        domain_interval_seconds=DEFAULT_DOMAIN_INTERVAL_SECONDS,
        bootstrap_request_reservation=80,
        proxy_file="/opt/airflow/proxys.txt",
    )

    pipeline.fetch_wave(
        str(uuid.UUID(int=1)),
        worker_id="worker-1",
        page_kinds=["competition_index"],
        settings=settings,
    )

    assert captured["max_browser_requests"] == 80
    assert captured["max_browser_bytes"] == 16 * 1024 * 1024
    assert control.reservations[0][1]["requests"] == 82
    assert control.reservations[0][1]["bytes_"] == 23 * 1024 * 1024


def test_a_wave_after_the_budget_stop_no_ops_instead_of_raising(tmp_path):
    """The wave that follows a budget stop claims nothing, because the run handed
    its targets back to the queue. Counting those 'skipped' targets as unfinished
    made it raise — so a run that spent its budget still went red."""
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.claim_targets = lambda *args, **kwargs: []
    control.create_due_run_cohort = lambda *args, **kwargs: []
    control.get_run_summary = lambda *args, **kwargs: {
        "target_counts": {"succeeded": 11, "skipped": 14},
    }

    def forbidden(*_):
        raise AssertionError("no fetcher for an empty wave")

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=forbidden,
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        str(uuid.UUID(int=1)),
        worker_id="worker-1",
        page_kinds=["competition"],
        settings=_settings(),
    )

    assert result.claimed == 0
    assert result.failures == []


def test_a_resumed_run_with_terminally_failed_targets_no_ops_instead_of_deadlocking(
    tmp_path,
):
    """A 'failed' target is terminal for its run: it is not claimable and can
    never re-enter this run's cohort, while the page itself is already back in
    the frontier for later runs. Counting it as unfinished deadlocked every
    resumed run on the same FetchWaveError forever (#1102)."""
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.claim_targets = lambda *args, **kwargs: []
    control.create_due_run_cohort = lambda *args, **kwargs: []
    control.get_run_summary = lambda *args, **kwargs: {
        "target_counts": {"succeeded": 29, "skipped": 20, "failed": 1},
    }

    def forbidden(*_):
        raise AssertionError("no fetcher for an empty wave")

    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        fetcher_factory=forbidden,
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    result = pipeline.fetch_wave(
        str(uuid.UUID(int=1)),
        worker_id="worker-1",
        page_kinds=["competition"],
        settings=_settings(),
    )

    assert result.claimed == 0
    assert result.failures == []


def test_a_wave_with_claimable_backlog_still_raises_the_unfinished_gate(
    tmp_path,
):
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    control.claim_targets = lambda *args, **kwargs: []
    control.create_due_run_cohort = lambda *args, **kwargs: []
    control.get_run_summary = lambda *args, **kwargs: {
        "target_counts": {"succeeded": 1, "pending": 2, "leased": 1},
    }
    pipeline = FBrefPipeline(
        control,
        raw,
        generic_writer=FakeWriter(),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    with pytest.raises(FetchWaveError, match="3 unfinished"):
        pipeline.fetch_wave(
            str(uuid.UUID(int=1)),
            worker_id="worker-1",
            page_kinds=["competition"],
            settings=_settings(),
        )


def test_the_bootstrap_allowance_follows_the_run_budget_everywhere():
    """The fetch wave runs in a subprocess that rebuilds its settings from the
    command line, so an allowance computed only in the DAG never reached the
    browser. Deriving it from the run's own budget keeps every caller in step."""
    daily = PipelineSettings(run_type="current", request_limit=200)
    backfill = PipelineSettings(run_type="backfill", request_limit=25)

    assert daily.bootstrap_request_reservation == 80
    assert backfill.bootstrap_request_reservation == 20


def test_a_failed_validation_leaves_the_run_open_for_its_retry(tmp_path):
    """A finished run is terminal. Marking it failed on the first validation
    error made every retry of the task impossible: the retry re-validated
    cleanly and then died on 'run cannot finish as succeeded'. The DAG's failure
    callback aborts the run when the DAG itself gives up — that is the only
    point at which the outcome is known."""
    raw = _raw_store(tmp_path)
    control = FakeControl(raw)
    summary = control.get_run_summary(str(uuid.uuid4()))
    summary["session_metrics"] = {"max_bootstraps_per_session": 25}
    control.get_run_summary = lambda _, **__: summary
    pipeline = FBrefPipeline(control, raw, generic_writer=FakeWriter())
    run_id = str(uuid.uuid4())

    with pytest.raises(RunValidationError):
        pipeline.validate_and_finish(run_id)
    assert not [event for event in control.events if event.startswith("finish:")]

    # The gate now passes (the operator fixed what it caught): the same run must
    # be able to finish green.
    summary["session_metrics"] = {"max_bootstraps_per_session": 1}
    pipeline.validate_and_finish(run_id)

    assert "finish:True" in control.events
