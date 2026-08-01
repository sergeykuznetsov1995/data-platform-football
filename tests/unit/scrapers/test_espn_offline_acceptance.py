"""Task 7 offline acceptance: one immutable ESPN Native evidence chain."""

from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
import socket

import pytest

from scrapers.espn.discovery import discover_catalog
from scrapers.espn.models import AgeClass
from scrapers.espn.raw_store import EspnRawStore
from scrapers.espn.registry import promote_candidate, validate_registry_document
from scrapers.espn.repository import render_current_view_sql, validate_scope_generation
from scrapers.espn.runner import execute
from tests.unit.scrapers.test_run_espn_scraper import (
    FakeHttpClient,
    FakeRepository,
    _bind_replay_to_capture,
    _plan,
    _rewrite_signed_plan,
    _scoreboard,
)


def _discovered_registry():
    dropdown = {"leagues": [{"id": "730", "slug": "ita.1", "name": "Italian Serie A"}]}
    detail = {
        "id": "730",
        "slug": "ita.1",
        "name": "Italian Serie A",
        "gender": "MALE",
        "genderEvidence": "frozen ESPN detail fixture",
        "season": {
            "year": 2020,
            "displayName": "2020-21 Italian Serie A",
            "startDate": "2020-08-01T00:00Z",
            "endDate": "2021-07-31T23:59Z",
        },
        "capabilities": {
            "schedule": "proven",
            "lineup": "proven",
            "matchsheet": "proven",
        },
    }
    snapshot = discover_catalog(
        dropdown,
        details_by_slug={"ita.1": detail},
        captured_at="2026-07-31T12:00:00+00:00",
    )
    promoted = promote_candidate(
        {
            "schema_version": 1,
            "registry_version": "offline-acceptance-v1",
            "as_of": "2026-07-31",
            "competitions": [],
        },
        snapshot.candidates[0],
        age_class=AgeClass.SENIOR,
        age_class_evidence=("reviewed fixture",),
        legacy_league="ITA-Serie A",
    )
    return snapshot, validate_registry_document(promoted)


def _bind_registry(options, plan, registry):
    registry_uri = plan.metadata["runtime"]["registry_snapshot_uri"]
    Path(registry_uri.removeprefix("file://")).write_text(
        registry.canonical_json(), encoding="utf-8"
    )
    return _rewrite_signed_plan(
        options,
        plan,
        lambda document: document.__setitem__(
            "registry_signature", registry.signature()
        ),
    )


@pytest.mark.unit
def test_discovery_capture_parse_publish_current_view_and_exact_replay_are_offline(
    tmp_path, monkeypatch
):
    def network_forbidden(*_args, **_kwargs):
        raise AssertionError("offline acceptance attempted a real network connection")

    monkeypatch.setattr(socket, "create_connection", network_forbidden)
    snapshot, registry = _discovered_registry()
    competition = registry.by_id[730]
    edition = competition.current_edition
    assert snapshot.candidates[0].source_season_year == 2020
    assert registry.signature()

    capture_options, capture_plan = _plan(
        tmp_path / "capture",
        "backfill",
        ((competition, edition),),
        run_id="offline-capture",
        as_of=date(2020, 9, 20),
    )
    capture_plan = _bind_registry(capture_options, capture_plan, registry)
    signed_envelope = json.loads(
        Path(capture_options.plan_uri.removeprefix("file://")).read_text()
    )
    assert signed_envelope["signature"] == capture_plan.signature()
    assert signed_envelope["plan"]["registry_signature"] == registry.signature()

    raw_store = EspnRawStore.from_uri(capture_options.raw_store_uri)
    local_capture = FakeHttpClient(
        raw_store,
        {competition.slug: _scoreboard(competition, edition)},
    )
    capture_repository = FakeRepository()
    capture = execute(
        capture_options,
        repository=capture_repository,
        raw_store=raw_store,
        http_client=local_capture,
    )

    assert capture.exit_code == 0
    assert capture.payload["state"] == "complete"
    generation = capture_repository.generations[0]
    assert validate_scope_generation(generation).passed
    published = capture.payload["scopes"][0]
    assert published["manifest_sha256"] == generation.manifest_sha256
    assert published["generation_signature"] == generation.generation_signature
    assert all(item.raw_uri.startswith("file://") for item in generation.raw_ledger)

    current_sql = "\n".join(
        render_current_view_sql(entity)
        for entity in ("schedule", "lineup", "matchsheet")
    )
    for fence in (
        "espn_ingest_manifest_v2",
        "espn_scope_cutover_v2",
        "status = 'complete'",
        "native_generation_id",
        "native_generation_signature",
        "native_manifest_sha256",
    ):
        assert fence in current_sql

    raw_manifest_path = Path(capture_options.raw_manifest_uri.removeprefix("file://"))
    raw_manifest_bytes = raw_manifest_path.read_bytes()
    replay_source = {
        "mode": "backfill",
        "run_id": capture_options.run_id,
        "attempt": capture_options.attempt,
        "plan_signature": capture_plan.signature(),
        "raw_manifest_sha256": hashlib.sha256(raw_manifest_bytes).hexdigest(),
    }
    replay_options, replay_plan = _plan(
        tmp_path / "replay",
        "replay",
        ((competition, edition),),
        run_id="offline-replay",
        as_of=date(2020, 9, 20),
        replay_source=replay_source,
    )
    replay_plan = _bind_registry(replay_options, replay_plan, registry)
    replay_options, _ = _bind_replay_to_capture(
        replay_options, replay_plan, capture_options
    )
    for alias in (
        Path(capture_options.raw_store_uri.removeprefix("file://")) / "targets"
    ).rglob("*.json"):
        alias.write_text('{"forged":true}', encoding="utf-8")

    replay_repository = FakeRepository()
    replay = execute(
        replay_options,
        repository=replay_repository,
        raw_store=raw_store,
    )

    assert replay.exit_code == 0
    replayed = replay_repository.generations[0]
    assert replayed.schedule == generation.schedule
    assert replayed.lineup == generation.lineup
    assert replayed.matchsheet == generation.matchsheet
    assert [item.raw_sha256 for item in replayed.raw_ledger] == [
        item.raw_sha256 for item in generation.raw_ledger
    ]
