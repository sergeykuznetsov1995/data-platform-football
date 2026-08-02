"""Network-free contracts for the plan-driven ESPN Native Bronze runner."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from scrapers.espn.models import (
    AgeClass,
    CapabilityState,
    Competition,
    DispositionState,
    Edition,
    EntityCapabilities,
    Gender,
    IngestPlan,
    RequestDisposition,
    ScopePlan,
)
from scrapers.espn.parser_contracts import PARSER_VERSION
from scrapers.espn.parsers import parse_scoreboards, parse_summary
from scrapers.espn.raw_store import EspnRawStore
from scrapers.espn.registry import Registry
from scrapers.espn.repository import (
    RawLedgerRecord,
    ScopeGeneration,
    ScopePublicationResult,
    ScopePublicationState,
    canonical_json,
    validate_scope_generation,
)
from scrapers.espn.runner import (
    ArtifactConflictError,
    ExecutionOptions,
    RunnerConfigurationError,
    execute,
    is_full_reconciliation_day,
    scope_snapshot_bytes,
    stage,
)
from scrapers.espn.transport_contracts import (
    EndpointType,
    FetchResult,
    canonicalize_target,
)


FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "espn"
UTC = timezone.utc
NOW = datetime(2026, 7, 31, 9, tzinfo=UTC)


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _capabilities(state: CapabilityState = CapabilityState.PROVEN):
    return EntityCapabilities(
        schedule=CapabilityState.PROVEN,
        lineup=state,
        matchsheet=state,
    )


def _competition(
    espn_id: int = 730,
    slug: str = "ita.1",
    *,
    source_year: int = 2020,
    start: date = date(2020, 8, 1),
    end: date = date(2021, 7, 31),
    capabilities: EntityCapabilities | None = None,
) -> tuple[Competition, Edition]:
    edition = Edition(
        source_season_year=source_year,
        display_name=f"{source_year}/{str(source_year + 1)[-2:]}",
        start_date=start,
        end_date=end,
        current=True,
        capabilities=capabilities or _capabilities(),
    )
    competition = Competition(
        espn_id=espn_id,
        slug=slug,
        name=f"League {espn_id}",
        gender=Gender.MALE,
        age_class=AgeClass.SENIOR,
        enabled=True,
        editions=(edition,),
        gender_evidence=("fixture",),
        age_class_evidence=("manual",),
    )
    return competition, edition


def _scope(competition: Competition, edition: Edition) -> ScopePlan:
    return ScopePlan(
        scope_id=competition.scope_id(edition),
        espn_id=competition.espn_id,
        slug=competition.slug,
        source_season_year=edition.source_season_year,
        start_date=edition.start_date,
        end_date=edition.end_date,
        capabilities=edition.capabilities,
    )


def _scoreboard(
    competition: Competition,
    edition: Edition,
    *,
    event_ids: tuple[int, ...] = (401000001,),
    event_date: str = "2020-09-19T18:45Z",
    status: str = "STATUS_FULL_TIME",
) -> bytes:
    document = _fixture("native_scoreboard.json")
    document["leagues"][0]["id"] = str(competition.espn_id)
    document["leagues"][0]["slug"] = competition.slug
    prototype = document["events"][0]
    events = []
    for event_id in event_ids:
        event = deepcopy(prototype)
        event["id"] = str(event_id)
        event["date"] = event_date
        event["season"]["year"] = edition.source_season_year
        event["status"]["type"]["name"] = status
        event["status"]["type"]["completed"] = status == "STATUS_FULL_TIME"
        if status != "STATUS_FULL_TIME":
            for side in event["competitions"][0]["competitors"]:
                side.pop("score", None)
        events.append(event)
    document["events"] = events
    return json.dumps(document, sort_keys=True).encode()


def _summary(event_id: int) -> bytes:
    document = _fixture("native_summary.json")
    document["header"]["id"] = str(event_id)
    return json.dumps(document, sort_keys=True).encode()


def _empty_summary(event_id: int) -> bytes:
    return json.dumps(
        {
            "header": {
                "id": str(event_id),
                "competitions": [
                    {
                        "date": "2020-09-19T18:45Z",
                        "competitors": [
                            {
                                "homeAway": "home",
                                "team": {"id": "10", "displayName": "Home FC"},
                            },
                            {
                                "homeAway": "away",
                                "team": {"id": "20", "displayName": "Away FC"},
                            },
                        ],
                    }
                ],
            }
        },
        sort_keys=True,
    ).encode()


class FakeHttpClient:
    def __init__(
        self,
        raw_store: EspnRawStore,
        scoreboard_by_slug,
        *,
        fail_slug=None,
        summary_factory=_summary,
    ):
        self.raw_store = raw_store
        self.scoreboard_by_slug = scoreboard_by_slug
        self.fail_slug = fail_slug
        self.summary_factory = summary_factory
        self.calls: list[tuple[str, EndpointType, dict]] = []

    def fetch_json(
        self,
        url,
        endpoint,
        params=None,
        *,
        competition_id=None,
        event_id=None,
        force_refresh=False,
    ):
        endpoint = EndpointType.parse(endpoint)
        params = dict(params or {})
        self.calls.append((url, endpoint, params))
        slug = url.split("/soccer/", 1)[1].split("/", 1)[0]
        if slug == self.fail_slug:
            raise RuntimeError("injected transport failure")
        body = (
            self.scoreboard_by_slug[slug]
            if endpoint is EndpointType.SCOREBOARD
            else self.summary_factory(int(event_id))
        )
        target = canonicalize_target(url, params)
        record = self.raw_store.store(
            target,
            endpoint,
            body,
            fetched_at="2026-07-31T08:00:00+00:00",
            direct_bytes=len(body),
        )
        return FetchResult(
            target=target,
            endpoint=endpoint,
            json_data=json.loads(body),
            body=body,
            attempts=1,
            status=200,
            cache_hit=False,
            direct_bytes=len(body),
            proxy_bytes=0,
            raw_uri=record.raw_uri,
            content_hash=record.content_hash,
            fetched_at=record.fetched_at,
        )


class FakeRepository:
    def __init__(self, *, fail_scope: str | None = None):
        self.fail_scope = fail_scope
        self.generations: list[ScopeGeneration] = []

    def publish_scope(self, generation: ScopeGeneration):
        report = validate_scope_generation(generation)
        assert report.passed, report.failures
        if generation.plan.scope_id == self.fail_scope:
            raise RuntimeError("injected publication failure")
        self.generations.append(generation)
        return ScopePublicationResult(
            scope_id=generation.plan.scope_id,
            generation_id=generation.generation_id,
            state=ScopePublicationState.PUBLISHED,
            manifest_sha256=generation.manifest_sha256,
        )


def _write(path: Path, payload: bytes | str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload.encode() if isinstance(payload, str) else payload)
    return path.as_uri()


def _registry(tmp_path: Path, competitions: tuple[Competition, ...]) -> tuple[str, str]:
    registry = Registry(
        schema_version=1,
        registry_version="fixture-v1",
        as_of=date(2026, 7, 31),
        competitions=competitions,
    )
    uri = _write(tmp_path / "registry.json", registry.canonical_json())
    return uri, registry.signature()


def _prior_generation(
    competition: Competition,
    edition: Edition,
    *,
    event_ids: tuple[int, ...] = (401000001,),
) -> ScopeGeneration:
    scope = _scope(competition, edition)
    schedule = parse_scoreboards(
        _scoreboard(competition, edition, event_ids=event_ids),
        competition=competition,
        edition=edition,
        query_start=edition.start_date,
        query_end=edition.end_date,
    )
    summaries = [
        parse_summary(
            _summary(event.event_id),
            competition=competition,
            edition=edition,
            event=event,
        )
        for event in schedule
    ]
    scoreboard_hash = hashlib.sha256(b"prior-scoreboard").hexdigest()
    ledger = [
        RawLedgerRecord(
            request_id="scoreboard:prior",
            endpoint="scoreboard",
            event_id=None,
            disposition=DispositionState.CAPTURED,
            raw_uri="s3://raw/prior-scoreboard.json.gz",
            raw_sha256=scoreboard_hash,
            fetched_at=NOW - timedelta(days=1),
            direct_bytes=100,
            proxy_bytes=0,
            event_ids=tuple(event.event_id for event in schedule),
        )
    ]
    dispositions = []
    for event, summary in zip(schedule, summaries):
        ledger.append(
            RawLedgerRecord(
                request_id=f"summary:{event.event_id}",
                endpoint="summary",
                event_id=event.event_id,
                disposition=DispositionState.CAPTURED,
                raw_uri=f"s3://raw/prior-summary-{event.event_id}.json.gz",
                raw_sha256=hashlib.sha256(str(event.event_id).encode()).hexdigest(),
                fetched_at=NOW - timedelta(days=1),
                direct_bytes=200,
                proxy_bytes=0,
            )
        )
        for entity, state in (
            ("lineup", summary.lineup_state),
            ("matchsheet", summary.matchsheet_state),
        ):
            dispositions.append(
                RequestDisposition(
                    endpoint=entity,
                    state=DispositionState(state.value),
                    detail="prior complete",
                    event_id=event.event_id,
                )
            )
    return ScopeGeneration(
        plan=scope,
        run_id="prior-run",
        generation_id="prior-generation",
        registry_snapshot_uri="s3://registry/prior.json",
        registry_signature="a" * 64,
        plan_signature="b" * 64,
        parser_version=PARSER_VERSION,
        runtime_version="espn-native-runtime-v2",
        ingested_at=NOW - timedelta(days=1),
        batch_id="prior-batch",
        schedule=schedule,
        lineup=tuple(row for summary in summaries for row in summary.lineup),
        matchsheet=tuple(row for summary in summaries for row in summary.matchsheet),
        planned_request_ids=tuple(item.request_id for item in ledger),
        raw_ledger=tuple(ledger),
        dispositions=tuple(dispositions),
    )


def _plan(
    tmp_path: Path,
    mode: str,
    competitions: tuple[tuple[Competition, Edition], ...],
    *,
    attempt: int = 1,
    run_id: str = "run-1",
    as_of: date = date(2020, 9, 20),
    initial_capture: bool = True,
    priors: dict[str, ScopeGeneration] | None = None,
    active: dict[str, bool] | None = None,
    known_nonterminal_events: dict[str, list[dict]] | None = None,
    replay_source: dict | None = None,
    max_events: int = 100,
    selected_scopes: tuple[str, ...] | None = None,
) -> tuple[ExecutionOptions, IngestPlan]:
    registry_uri, registry_signature = _registry(
        tmp_path, tuple(item[0] for item in competitions)
    )
    raw_manifest = tmp_path / "runs" / run_id / f"attempt-{attempt}" / "raw.json"
    output = tmp_path / "runs" / run_id / f"attempt-{attempt}" / "result.json"
    bindings = {}
    priors = priors or {}
    active = active or {}
    known_nonterminal_events = known_nonterminal_events or {}
    for competition, edition in competitions:
        scope = _scope(competition, edition)
        prior = priors.get(scope.scope_id)
        prior_binding = None
        if prior is not None:
            prior_path = tmp_path / "prior" / f"{scope.scope_id}.json"
            prior_bytes = scope_snapshot_bytes(prior)
            prior_uri = _write(prior_path, prior_bytes)
            prior_binding = {
                "uri": prior_uri,
                "artifact_sha256": hashlib.sha256(prior_bytes).hexdigest(),
                "scope_id": scope.scope_id,
                "generation_id": prior.generation_id,
                "generation_signature": prior.generation_signature,
                "manifest_sha256": prior.manifest_sha256,
            }
        bindings[scope.scope_id] = {
            "active": active.get(scope.scope_id, True),
            "initial_capture": initial_capture,
            "generation_id": f"generation-{scope.scope_id.replace(':', '-')}",
            "batch_id": f"batch-{scope.scope_id.replace(':', '-')}",
            "ingested_at": NOW.isoformat().replace("+00:00", "Z"),
            "generation_snapshot_uri": (
                tmp_path
                / "runs"
                / run_id
                / f"attempt-{attempt}"
                / f"scope-{scope.scope_id}.json"
            ).as_uri(),
            "known_nonterminal_events": known_nonterminal_events.get(
                scope.scope_id, []
            ),
            "prior": prior_binding,
        }
    metadata = {
        "runtime": {
            "mode": mode,
            "attempt": attempt,
            "registry_snapshot_uri": registry_uri,
            "raw_manifest_uri": raw_manifest.as_uri(),
            "output_uri": output.as_uri(),
            "raw_store_uri": (tmp_path / "raw-store").as_uri(),
            "max_events": max_events,
            "selected_scopes": list(
                selected_scopes if selected_scopes is not None else sorted(bindings)
            ),
            "scope_bindings": bindings,
            "replay_source": replay_source,
        }
    }
    plan = IngestPlan(
        schema_version=1,
        run_id=run_id,
        as_of=as_of,
        registry_signature=registry_signature,
        scopes=tuple(_scope(*item) for item in competitions),
        metadata=metadata,
    )
    envelope = {
        "kind": "espn-ingest-plan-v1",
        "plan": plan.to_dict(),
        "signature": plan.signature(),
    }
    plan_uri = _write(
        tmp_path / "plans" / f"{run_id}.json",
        json.dumps(envelope, sort_keys=True, separators=(",", ":")),
    )
    options = ExecutionOptions(
        mode=mode,
        scopes=(
            selected_scopes
            if selected_scopes is not None
            else tuple(scope.scope_id for scope in plan.scopes)
        ),
        as_of=as_of,
        run_id=run_id,
        attempt=attempt,
        plan_uri=plan_uri,
        raw_manifest_uri=raw_manifest.as_uri(),
        output_uri=output.as_uri(),
        raw_store_uri=(tmp_path / "raw-store").as_uri(),
        max_events=max_events,
    )
    return options, plan


def _rewrite_signed_plan(options, plan: IngestPlan, mutate):
    document = plan.to_dict()
    mutate(document)
    rebuilt = IngestPlan(
        schema_version=document["schema_version"],
        run_id=document["run_id"],
        as_of=date.fromisoformat(document["as_of"]),
        registry_signature=document["registry_signature"],
        scopes=plan.scopes,
        metadata=document["metadata"],
    )
    envelope = {
        "kind": "espn-ingest-plan-v1",
        "plan": rebuilt.to_dict(),
        "signature": rebuilt.signature(),
    }
    Path(options.plan_uri.removeprefix("file://")).write_text(
        json.dumps(envelope, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return rebuilt


def _reseal_raw_manifest(path: Path, mutate) -> None:
    manifest = json.loads(path.read_text())
    mutate(manifest)
    base = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    manifest["manifest_sha256"] = hashlib.sha256(
        (canonical_json(base) + "\n").encode()
    ).hexdigest()
    path.write_text(canonical_json(manifest) + "\n", encoding="utf-8")


def _bind_replay_to_capture(options, plan, capture_options):
    def mutate(document):
        runtime = document["metadata"]["runtime"]
        runtime["raw_manifest_uri"] = capture_options.raw_manifest_uri
        runtime["raw_store_uri"] = capture_options.raw_store_uri

    rebuilt = _rewrite_signed_plan(options, plan, mutate)
    return (
        replace(
            options,
            raw_manifest_uri=capture_options.raw_manifest_uri,
            raw_store_uri=capture_options.raw_store_uri,
        ),
        rebuilt,
    )


@pytest.mark.unit
@pytest.mark.parametrize("mode", ["daily", "repair", "backfill", "replay"])
def test_cli_exposes_only_native_modes_and_rejects_legacy_flags(mode, tmp_path):
    from dags.scripts.run_espn_scraper import build_parser

    parser = build_parser()
    argv = [
        mode,
        "--scope",
        "730:2020",
        "--as-of",
        "2020-09-20",
        "--run-id",
        "run-1",
        "--attempt",
        "1",
        "--plan-uri",
        (tmp_path / "plan.json").as_uri(),
        "--raw-manifest-uri",
        (tmp_path / "raw.json").as_uri(),
        "--output",
        (tmp_path / "result.json").as_uri(),
        "--raw-store-uri",
        (tmp_path / "raw").as_uri(),
        "--max-events",
        "50",
    ]
    args = parser.parse_args(argv)
    assert args.mode == mode
    assert args.scope == ["730:2020"]
    assert os.access(
        Path(__file__).resolve().parents[3] / "dags/scripts/run_espn_scraper.py",
        os.X_OK,
    )
    for legacy in ("--leagues", "--season", "--force-replace"):
        with pytest.raises(SystemExit):
            parser.parse_args([*argv, legacy, "legacy"])


@pytest.mark.unit
def test_runner_rejects_run_attempt_scope_and_registry_drift_before_io(tmp_path):
    competition, edition = _competition()
    options, _ = _plan(tmp_path, "daily", ((competition, edition),))
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    client = FakeHttpClient(
        raw_store, {competition.slug: _scoreboard(competition, edition)}
    )
    repository = FakeRepository()

    for drifted in (
        replace(options, run_id="wrong-run"),
        replace(options, attempt=2),
        replace(options, scopes=("999:2020",)),
        replace(options, mode="repair"),
    ):
        with pytest.raises(RunnerConfigurationError):
            execute(
                drifted, repository=repository, raw_store=raw_store, http_client=client
            )
    assert client.calls == []
    assert repository.generations == []

    registry_path = (
        Path(options.plan_uri.removeprefix("file://")).parents[1] / "registry.json"
    )
    registry_path.write_text('{"forged":true}', encoding="utf-8")
    with pytest.raises(RunnerConfigurationError, match="registry"):
        execute(options, repository=repository, raw_store=raw_store, http_client=client)
    assert client.calls == []


@pytest.mark.unit
def test_initial_capture_fetches_full_calendar_and_one_summary_for_both_entities(
    tmp_path,
):
    competition, edition = _competition()
    options, _ = _plan(tmp_path, "daily", ((competition, edition),))
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    client = FakeHttpClient(
        raw_store, {competition.slug: _scoreboard(competition, edition)}
    )
    repository = FakeRepository()

    result = execute(
        options,
        repository=repository,
        raw_store=raw_store,
        http_client=client,
    )

    assert result.exit_code == 0
    generation = repository.generations[0]
    assert generation.schedule and generation.lineup and generation.matchsheet
    summary_calls = [call for call in client.calls if call[1] is EndpointType.SUMMARY]
    assert len(summary_calls) == 1
    scoreboard_calls = [
        call for call in client.calls if call[1] is EndpointType.SCOREBOARD
    ]
    assert len(scoreboard_calls) == 1
    scoreboard_call = scoreboard_calls[0]
    assert scoreboard_call[2]["dates"] == "20200801-20210731"
    assert Path(options.output_uri.removeprefix("file://")).is_file()
    assert not list(tmp_path.rglob("*.tmp-*"))


@pytest.mark.unit
def test_initial_capture_splits_calendar_longer_than_supported_scoreboard_window():
    from scrapers.espn import runner

    competition, edition = _competition(
        espn_id=3908,
        slug="caf.nations",
        source_year=2025,
        start=date(2025, 12, 21),
        end=date(2026, 12, 31),
    )
    scope = _scope(competition, edition)
    binding = runner.ScopeBinding(
        active=True,
        initial_capture=True,
        generation_id="generation-3908-2025",
        batch_id="batch-3908-2025",
        ingested_at=NOW,
        generation_snapshot_uri="s3://artifacts/generation.json",
        known_nonterminal_events=(),
        prior=None,
    )

    requests = runner._scoreboard_requests(
        scope,
        binding,
        as_of=date(2026, 7, 31),
        mode="daily",
    )

    assert [(request.query_start, request.query_end) for request in requests] == [
        (date(2025, 12, 21), date(2026, 12, 20)),
        (date(2026, 12, 21), date(2026, 12, 31)),
    ]
    assert [request.params["dates"] for request in requests] == [
        "20251221-20261220",
        "20261221-20261231",
    ]


@pytest.mark.unit
def test_offline_stage_uses_complete_exact_raw_without_http_or_publication(tmp_path):
    competition, edition = _competition()
    options, _ = _plan(tmp_path, "daily", ((competition, edition),))
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    client = FakeHttpClient(
        raw_store, {competition.slug: _scoreboard(competition, edition)}
    )
    captured_repository = FakeRepository()
    execute(
        options,
        repository=captured_repository,
        raw_store=raw_store,
        http_client=client,
    )
    captured_calls = tuple(client.calls)

    result = stage(options, raw_store=raw_store)

    assert result.exit_code == 0
    assert result.payload["state"] == "staged"
    assert tuple(result.generations) == (competition.scope_id(edition),)
    assert result.generations[competition.scope_id(edition)].schedule
    assert tuple(client.calls) == captured_calls


@pytest.mark.unit
def test_daily_window_known_nonterminal_and_sha256_full_shard_are_deterministic(
    tmp_path,
):
    competition, edition = _competition()
    prior = _prior_generation(competition, edition)
    scope_id = competition.scope_id(edition)
    known = {scope_id: [{"event_id": 999, "event_date": "2020-11-01"}]}
    options, _ = _plan(
        tmp_path,
        "daily",
        ((competition, edition),),
        initial_capture=False,
        priors={scope_id: prior},
        known_nonterminal_events=known,
    )
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    client = FakeHttpClient(
        raw_store,
        {
            competition.slug: _scoreboard(
                competition,
                edition,
                event_ids=(),
                status="STATUS_SCHEDULED",
            )
        },
    )
    repository = FakeRepository()
    result = execute(
        options, repository=repository, raw_store=raw_store, http_client=client
    )
    assert result.exit_code == 1
    assert "known non-terminal" in result.payload["scopes"][0]["error"]
    assert repository.generations == []
    date_queries = {
        call[2]["dates"] for call in client.calls if call[1] is EndpointType.SCOREBOARD
    }
    if is_full_reconciliation_day(scope_id, options.as_of):
        assert date_queries == {"20200801-20210731"}
    else:
        assert date_queries == {"20200917-20201004", "20201101"}

    days = [options.as_of + timedelta(days=offset) for offset in range(7)]
    assert sum(is_full_reconciliation_day(scope_id, day) for day in days) == 1
    assert is_full_reconciliation_day(scope_id, days[0]) == (
        int(hashlib.sha256(scope_id.encode()).hexdigest(), 16) % 7
        == days[0].toordinal() % 7
    )


@pytest.mark.unit
def test_daily_full_generation_preserves_out_of_window_rows_and_prior_raw_bindings(
    tmp_path,
):
    competition, edition = _competition()
    prior = _prior_generation(competition, edition, event_ids=(401000001, 401000002))
    outside_id = 401000002
    outside_kickoff = datetime(2020, 12, 19, 18, 45, tzinfo=UTC)
    prior = ScopeGeneration(
        **{
            **prior.constructor_values(),
            "schedule": tuple(
                replace(
                    row,
                    kickoff=outside_kickoff,
                    date=outside_kickoff,
                    match_date=outside_kickoff,
                )
                if row.event_id == outside_id
                else row
                for row in prior.schedule
            ),
        }
    )
    outside_schedule = next(row for row in prior.schedule if row.event_id == outside_id)
    outside_lineup = tuple(row for row in prior.lineup if row.event_id == outside_id)
    outside_matchsheet = tuple(
        row for row in prior.matchsheet if row.event_id == outside_id
    )
    scope_id = competition.scope_id(edition)
    # Pick a non-reconciliation day so this specifically exercises the daily merge.
    as_of = date(2020, 9, 20)
    while is_full_reconciliation_day(scope_id, as_of):
        as_of += timedelta(days=1)
    options, _ = _plan(
        tmp_path,
        "daily",
        ((competition, edition),),
        as_of=as_of,
        initial_capture=False,
        priors={scope_id: prior},
    )
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    client = FakeHttpClient(
        raw_store,
        {competition.slug: _scoreboard(competition, edition, event_ids=(401000001,))},
    )
    repository = FakeRepository()

    result = execute(
        options, repository=repository, raw_store=raw_store, http_client=client
    )

    assert result.exit_code == 0
    generation = repository.generations[0]
    assert outside_schedule in generation.schedule
    assert set(outside_lineup).issubset(generation.lineup)
    assert set(outside_matchsheet).issubset(generation.matchsheet)
    outside_scoreboard_bindings = [
        item
        for item in generation.raw_ledger
        if item.endpoint == "scoreboard" and outside_id in item.event_ids
    ]
    assert len(outside_scoreboard_bindings) == 1
    assert outside_scoreboard_bindings[0].request_id == "scoreboard:prior"


@pytest.mark.unit
def test_overlapping_scoreboards_bind_every_event_to_exactly_one_raw_record(tmp_path):
    competition, edition = _competition()
    prior = _prior_generation(competition, edition)
    scope_id = competition.scope_id(edition)
    as_of = date(2020, 9, 20)
    while is_full_reconciliation_day(scope_id, as_of):
        as_of += timedelta(days=1)
    options, _ = _plan(
        tmp_path,
        "daily",
        ((competition, edition),),
        as_of=as_of,
        initial_capture=False,
        priors={scope_id: prior},
        known_nonterminal_events={
            scope_id: [{"event_id": 401000001, "event_date": "2020-09-19"}]
        },
    )
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    client = FakeHttpClient(
        raw_store,
        {competition.slug: _scoreboard(competition, edition)},
    )
    repository = FakeRepository()

    execute(options, repository=repository, raw_store=raw_store, http_client=client)

    generation = repository.generations[0]
    bindings = [
        record
        for record in generation.raw_ledger
        if record.endpoint == "scoreboard" and 401000001 in record.event_ids
    ]
    assert len(bindings) == 1
    assert validate_scope_generation(generation).passed


@pytest.mark.unit
def test_101_final_events_checkpoint_as_50_50_1_and_resume_without_refetch(tmp_path):
    competition, edition = _competition(
        capabilities=_capabilities(CapabilityState.PARTIAL)
    )
    event_ids = tuple(range(401100001, 401100102))
    options, _ = _plan(tmp_path, "backfill", ((competition, edition),))
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    body = _scoreboard(competition, edition, event_ids=event_ids)
    first_client = FakeHttpClient(
        raw_store, {competition.slug: body}, summary_factory=_empty_summary
    )
    first_repo = FakeRepository()

    first = execute(
        options,
        repository=first_repo,
        raw_store=raw_store,
        http_client=first_client,
    )
    assert first.exit_code != 0
    assert first_repo.generations == []
    assert sum(call[1] is EndpointType.SUMMARY for call in first_client.calls) == 100

    second_client = FakeHttpClient(
        raw_store, {competition.slug: body}, summary_factory=_empty_summary
    )
    second_repo = FakeRepository()
    second = execute(
        options,
        repository=second_repo,
        raw_store=raw_store,
        http_client=second_client,
    )
    assert second.exit_code == 0
    assert sum(call[1] is EndpointType.SUMMARY for call in second_client.calls) == 1
    assert not any(call[1] is EndpointType.SCOREBOARD for call in second_client.calls)
    raw_manifest = json.loads(
        Path(options.raw_manifest_uri.removeprefix("file://")).read_text()
    )
    summary_sizes = [
        len(checkpoint["requests"])
        for checkpoint in raw_manifest["checkpoints"]
        if checkpoint["endpoint"] == "summary"
    ]
    assert summary_sizes == [50, 50, 1]


@pytest.mark.unit
def test_replay_uses_bound_exact_blobs_after_alias_moves_and_never_http(tmp_path):
    competition, edition = _competition()
    capture_options, capture_plan = _plan(
        tmp_path / "capture", "backfill", ((competition, edition),)
    )
    raw_store = EspnRawStore.from_uri(capture_options.raw_store_uri)
    capture_client = FakeHttpClient(
        raw_store, {competition.slug: _scoreboard(competition, edition)}
    )
    assert (
        execute(
            capture_options,
            repository=FakeRepository(),
            raw_store=raw_store,
            http_client=capture_client,
        ).exit_code
        == 0
    )
    manifest_path = Path(capture_options.raw_manifest_uri.removeprefix("file://"))
    manifest_bytes = manifest_path.read_bytes()
    replay_source = {
        "mode": "backfill",
        "run_id": capture_options.run_id,
        "attempt": capture_options.attempt,
        "plan_signature": capture_plan.signature(),
        "raw_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
    }
    replay_options, _ = _plan(
        tmp_path / "replay",
        "replay",
        ((competition, edition),),
        run_id="replay-run",
        replay_source=replay_source,
    )
    replay_options = replace(
        replay_options,
        raw_manifest_uri=capture_options.raw_manifest_uri,
        raw_store_uri=capture_options.raw_store_uri,
    )
    # Rewrite the replay plan so its signed URI matches the exact source manifest.
    envelope_path = Path(replay_options.plan_uri.removeprefix("file://"))
    envelope = json.loads(envelope_path.read_text())
    envelope["plan"]["metadata"]["runtime"]["raw_manifest_uri"] = (
        capture_options.raw_manifest_uri
    )
    envelope["plan"]["metadata"]["runtime"]["raw_store_uri"] = (
        capture_options.raw_store_uri
    )
    unsigned = envelope["plan"]
    # Rebuild through the public model to obtain its canonical signature.
    replay_plan = IngestPlan(
        schema_version=unsigned["schema_version"],
        run_id=unsigned["run_id"],
        as_of=date.fromisoformat(unsigned["as_of"]),
        registry_signature=unsigned["registry_signature"],
        scopes=tuple(_scope(competition, edition) for _ in unsigned["scopes"]),
        metadata=unsigned["metadata"],
    )
    envelope["signature"] = replay_plan.signature()
    envelope_path.write_text(
        json.dumps(envelope, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )

    # Point mutable target aliases at bad JSON. Exact blob replay must ignore them.
    for target_file in (
        Path(capture_options.raw_store_uri.removeprefix("file://")) / "targets"
    ).rglob("*.json"):
        target_file.write_text('{"forged":true}', encoding="utf-8")

    replay_repo = FakeRepository()
    result = execute(replay_options, repository=replay_repo, raw_store=raw_store)
    assert result.exit_code == 0
    assert replay_repo.generations[0].schedule[0].event_id == 401000001


@pytest.mark.unit
def test_clean_noop_requires_bound_prior_complete_identity(tmp_path, monkeypatch):
    competition, edition = _competition()
    scope_id = competition.scope_id(edition)
    without_prior, _ = _plan(
        tmp_path / "bad",
        "daily",
        ((competition, edition),),
        initial_capture=False,
        active={scope_id: False},
    )
    raw_store = EspnRawStore.from_uri(without_prior.raw_store_uri)
    with pytest.raises(RunnerConfigurationError, match="prior COMPLETE"):
        execute(without_prior, repository=FakeRepository(), raw_store=raw_store)

    prior = _prior_generation(competition, edition)
    options, _ = _plan(
        tmp_path / "good",
        "daily",
        ((competition, edition),),
        initial_capture=False,
        priors={scope_id: prior},
        active={scope_id: False},
    )
    import scrapers.espn.runner as runner_module

    class ForbiddenDependency:
        def __init__(self, *args, **kwargs):
            raise AssertionError("clean no-op touched network/repository dependency")

        @classmethod
        def from_uri(cls, *args, **kwargs):
            raise AssertionError("clean no-op opened raw store")

    monkeypatch.setattr(runner_module, "EspnRawStore", ForbiddenDependency)
    monkeypatch.setattr(runner_module, "EspnBronzeRepository", ForbiddenDependency)
    result = execute(options)
    assert result.exit_code == 0
    assert result.payload["state"] == "noop"


@pytest.mark.unit
def test_empty_initial_scope_fails_but_good_scope_publishes_independently(tmp_path):
    first, first_edition = _competition(730, "ita.1")
    second, second_edition = _competition(731, "eng.1")
    options, _ = _plan(
        tmp_path,
        "repair",
        ((first, first_edition), (second, second_edition)),
    )
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    client = FakeHttpClient(
        raw_store,
        {
            first.slug: _scoreboard(
                first,
                first_edition,
                event_ids=(401000001,),
                status="STATUS_SCHEDULED",
            ),
            second.slug: _scoreboard(second, second_edition, event_ids=()),
        },
    )
    repository = FakeRepository()

    result = execute(
        options, repository=repository, raw_store=raw_store, http_client=client
    )

    assert result.exit_code != 0
    assert [item.plan.scope_id for item in repository.generations] == [
        first.scope_id(first_edition)
    ]
    states = {item["scope_id"]: item["state"] for item in result.payload["scopes"]}
    assert states[first.scope_id(first_edition)] == "complete"
    assert states[second.scope_id(second_edition)] == "incomplete"


@pytest.mark.unit
def test_retry_generation_identity_is_byte_stable_and_repository_idempotent(tmp_path):
    competition, edition = _competition()
    options, _ = _plan(tmp_path, "backfill", ((competition, edition),))
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    client = FakeHttpClient(
        raw_store, {competition.slug: _scoreboard(competition, edition)}
    )
    first_repo = FakeRepository()
    assert (
        execute(
            options, repository=first_repo, raw_store=raw_store, http_client=client
        ).exit_code
        == 0
    )
    first = first_repo.generations[0]

    class IdempotentRepository(FakeRepository):
        def publish_scope(self, generation):
            assert generation.generation_signature == first.generation_signature
            assert generation.manifest_sha256 == first.manifest_sha256
            return ScopePublicationResult(
                scope_id=generation.plan.scope_id,
                generation_id=generation.generation_id,
                state=ScopePublicationState.IDEMPOTENT,
                manifest_sha256=generation.manifest_sha256,
            )

    retry_client = FakeHttpClient(raw_store, {competition.slug: b"forbidden"})
    retry = execute(
        options,
        repository=IdempotentRepository(),
        raw_store=raw_store,
        http_client=retry_client,
    )
    assert retry.exit_code == 0
    assert retry_client.calls == []


@pytest.mark.unit
def test_signed_plan_tamper_unknown_metadata_and_resume_identity_fail_before_io(
    tmp_path,
):
    competition, edition = _competition()
    options, plan = _plan(tmp_path, "backfill", ((competition, edition),))
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    client = FakeHttpClient(
        raw_store, {competition.slug: _scoreboard(competition, edition)}
    )
    repository = FakeRepository()
    plan_path = Path(options.plan_uri.removeprefix("file://"))
    original = plan_path.read_text()
    tampered = json.loads(original)
    tampered["plan"]["run_id"] = "forged"
    plan_path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(RunnerConfigurationError, match="signature"):
        execute(options, repository=repository, raw_store=raw_store, http_client=client)
    assert client.calls == [] and repository.generations == []

    plan_path.write_text(original, encoding="utf-8")
    _rewrite_signed_plan(
        options,
        plan,
        lambda document: document["metadata"]["runtime"].__setitem__(
            "unknown", "forbidden"
        ),
    )
    with pytest.raises(RunnerConfigurationError, match="unknown"):
        execute(options, repository=repository, raw_store=raw_store, http_client=client)
    assert client.calls == []

    plan_path.write_text(original, encoding="utf-8")
    assert (
        execute(
            options, repository=repository, raw_store=raw_store, http_client=client
        ).exit_code
        == 0
    )
    raw_path = Path(options.raw_manifest_uri.removeprefix("file://"))
    _reseal_raw_manifest(raw_path, lambda manifest: manifest.__setitem__("attempt", 2))
    with pytest.raises(RunnerConfigurationError, match="resume raw manifest identity"):
        execute(
            options,
            repository=FakeRepository(),
            raw_store=raw_store,
            http_client=client,
        )


@pytest.mark.unit
@pytest.mark.parametrize("drift", ["hash", "generation"])
def test_prior_snapshot_hash_and_generation_drift_are_rejected(tmp_path, drift):
    competition, edition = _competition()
    prior = _prior_generation(competition, edition)
    scope_id = competition.scope_id(edition)
    options, plan = _plan(
        tmp_path,
        "daily",
        ((competition, edition),),
        initial_capture=False,
        priors={scope_id: prior},
    )
    prior_path = tmp_path / "prior" / f"{scope_id}.json"
    if drift == "hash":
        prior_path.write_bytes(prior_path.read_bytes() + b" ")
    else:
        envelope = json.loads(prior_path.read_text())
        envelope["generation_id"] = "forged-generation"
        rewritten = (canonical_json(envelope) + "\n").encode()
        prior_path.write_bytes(rewritten)

        def update_binding(document):
            binding = document["metadata"]["runtime"]["scope_bindings"][scope_id]
            binding["prior"]["artifact_sha256"] = hashlib.sha256(rewritten).hexdigest()

        _rewrite_signed_plan(options, plan, update_binding)
    with pytest.raises(RunnerConfigurationError, match="prior .*drift"):
        execute(
            options,
            repository=FakeRepository(),
            raw_store=EspnRawStore.from_uri(options.raw_store_uri),
        )


@pytest.mark.unit
def test_final_proven_summary_schema_failure_is_incomplete_while_good_scope_publishes(
    tmp_path,
):
    first, first_edition = _competition(730, "ita.1")
    second, second_edition = _competition(731, "eng.1")
    options, _ = _plan(
        tmp_path,
        "repair",
        ((first, first_edition), (second, second_edition)),
    )
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)

    def summaries(event_id: int) -> bytes:
        return _summary(event_id) if event_id == 401000001 else b"{}"

    client = FakeHttpClient(
        raw_store,
        {
            first.slug: _scoreboard(first, first_edition, event_ids=(401000001,)),
            second.slug: _scoreboard(second, second_edition, event_ids=(401000002,)),
        },
        summary_factory=summaries,
    )
    repository = FakeRepository()
    result = execute(
        options, repository=repository, raw_store=raw_store, http_client=client
    )
    assert result.exit_code == 1
    assert [item.plan.scope_id for item in repository.generations] == [
        first.scope_id(first_edition)
    ]
    by_scope = {item["scope_id"]: item for item in result.payload["scopes"]}
    assert by_scope[second.scope_id(second_edition)]["state"] == "incomplete"


@pytest.mark.unit
def test_successful_partial_absent_sections_publish_explicit_valid_empty(tmp_path):
    capabilities = EntityCapabilities(
        schedule=CapabilityState.PROVEN,
        lineup=CapabilityState.PARTIAL,
        matchsheet=CapabilityState.ABSENT,
    )
    competition, edition = _competition(capabilities=capabilities)
    options, _ = _plan(tmp_path, "backfill", ((competition, edition),))
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    client = FakeHttpClient(
        raw_store,
        {competition.slug: _scoreboard(competition, edition)},
        summary_factory=_empty_summary,
    )
    repository = FakeRepository()
    result = execute(
        options, repository=repository, raw_store=raw_store, http_client=client
    )
    assert result.exit_code == 0
    generation = repository.generations[0]
    assert generation.lineup == generation.matchsheet == ()
    assert {(item.endpoint, item.state) for item in generation.dispositions} == {
        ("lineup", DispositionState.VALID_EMPTY),
        ("matchsheet", DispositionState.VALID_EMPTY),
    }


@pytest.mark.unit
def test_each_summary_payload_is_parsed_exactly_once(tmp_path, monkeypatch):
    import scrapers.espn.runner as runner_module

    competition, edition = _competition()
    options, _ = _plan(tmp_path, "backfill", ((competition, edition),))
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    client = FakeHttpClient(
        raw_store, {competition.slug: _scoreboard(competition, edition)}
    )
    calls = []
    original = runner_module.parse_summary

    def counted(*args, **kwargs):
        calls.append(kwargs["event"].event_id)
        return original(*args, **kwargs)

    monkeypatch.setattr(runner_module, "parse_summary", counted)
    assert (
        execute(
            options,
            repository=FakeRepository(),
            raw_store=raw_store,
            http_client=client,
        ).exit_code
        == 0
    )
    assert calls == [401000001]


@pytest.mark.unit
@pytest.mark.parametrize("mode", ["repair", "backfill"])
def test_repair_and_backfill_always_read_full_edition_for_noninitial_scope(
    tmp_path, mode
):
    competition, edition = _competition()
    prior = _prior_generation(competition, edition)
    scope_id = competition.scope_id(edition)
    options, _ = _plan(
        tmp_path,
        mode,
        ((competition, edition),),
        initial_capture=False,
        priors={scope_id: prior},
    )
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    client = FakeHttpClient(
        raw_store,
        {
            competition.slug: _scoreboard(
                competition, edition, status="STATUS_SCHEDULED"
            )
        },
    )
    assert (
        execute(
            options,
            repository=FakeRepository(),
            raw_store=raw_store,
            http_client=client,
        ).exit_code
        == 0
    )
    scoreboard = next(
        call for call in client.calls if call[1] is EndpointType.SCOREBOARD
    )
    assert scoreboard[2]["dates"] == "20200801-20210731"


@pytest.mark.unit
def test_budget_duplicate_scope_and_signed_budget_drift_are_strict(tmp_path):
    competition, edition = _competition()
    options, _ = _plan(tmp_path, "backfill", ((competition, edition),))
    with pytest.raises(ValueError, match="max_events"):
        replace(options, max_events=101)
    with pytest.raises(ValueError, match="duplicate"):
        replace(options, scopes=("730:2020", "730:2020"))
    drifted = replace(options, max_events=50)
    with pytest.raises(RunnerConfigurationError, match="max_events"):
        execute(
            drifted,
            repository=FakeRepository(),
            raw_store=EspnRawStore.from_uri(options.raw_store_uri),
        )


@pytest.mark.unit
def test_artifact_uri_collisions_fail_preflight_before_publication(tmp_path):
    competition, edition = _competition()
    options, plan = _plan(tmp_path, "backfill", ((competition, edition),))

    def collide(document):
        runtime = document["metadata"]["runtime"]
        runtime["output_uri"] = runtime["raw_manifest_uri"]

    _rewrite_signed_plan(options, plan, collide)
    options = replace(options, output_uri=options.raw_manifest_uri)
    repository = FakeRepository()
    with pytest.raises(RunnerConfigurationError, match="artifact URI collision"):
        execute(options, repository=repository)
    assert repository.generations == []


@pytest.mark.unit
def test_signed_scope_selection_rejects_cli_subset_and_permuted_plan(tmp_path):
    first, first_edition = _competition(730, "ita.1")
    second, second_edition = _competition(731, "eng.1")
    competitions = ((first, first_edition), (second, second_edition))
    options, _ = _plan(tmp_path / "subset", "repair", competitions)
    with pytest.raises(RunnerConfigurationError, match="signed scope selection"):
        execute(
            replace(options, scopes=(first.scope_id(first_edition),)),
            repository=FakeRepository(),
        )

    reversed_ids = (
        second.scope_id(second_edition),
        first.scope_id(first_edition),
    )
    permuted, _ = _plan(
        tmp_path / "permuted",
        "repair",
        competitions,
        selected_scopes=reversed_ids,
    )
    with pytest.raises(RunnerConfigurationError, match="sorted"):
        execute(permuted, repository=FakeRepository())


@pytest.mark.unit
def test_signed_scope_selection_must_be_nonempty(tmp_path):
    competition, edition = _competition()
    options, _ = _plan(
        tmp_path,
        "repair",
        ((competition, edition),),
        selected_scopes=(),
    )

    with pytest.raises(RunnerConfigurationError, match="must not be empty"):
        execute(options, repository=FakeRepository())


@pytest.mark.unit
def test_cli_omitted_uses_signed_selection_and_distinct_batch_plans_are_usable(
    tmp_path,
):
    first, first_edition = _competition(730, "ita.1")
    second, second_edition = _competition(731, "eng.1")
    competitions = ((first, first_edition), (second, second_edition))
    scoreboard = {
        first.slug: _scoreboard(first, first_edition, status="STATUS_SCHEDULED"),
        second.slug: _scoreboard(second, second_edition, status="STATUS_SCHEDULED"),
    }
    published = []
    for label, selected in (
        ("a", first.scope_id(first_edition)),
        ("b", second.scope_id(second_edition)),
    ):
        options, _ = _plan(
            tmp_path / label,
            "repair",
            competitions,
            run_id=f"run-{label}",
            selected_scopes=(selected,),
        )
        if label == "a":
            options = replace(options, scopes=())
        raw_store = EspnRawStore.from_uri(options.raw_store_uri)
        repository = FakeRepository()
        result = execute(
            options,
            repository=repository,
            raw_store=raw_store,
            http_client=FakeHttpClient(raw_store, scoreboard),
        )
        assert result.exit_code == 0
        published.extend(item.plan.scope_id for item in repository.generations)
    assert published == [first.scope_id(first_edition), second.scope_id(second_edition)]


@pytest.mark.unit
def test_replay_selected_scope_accepts_verified_source_manifest_superset(tmp_path):
    first, first_edition = _competition(730, "ita.1")
    second, second_edition = _competition(731, "eng.1")
    competitions = ((first, first_edition), (second, second_edition))
    capture_options, capture_plan = _plan(
        tmp_path / "capture", "repair", competitions, run_id="capture-run"
    )
    raw_store = EspnRawStore.from_uri(capture_options.raw_store_uri)
    source_payloads = {
        first.slug: _scoreboard(first, first_edition, status="STATUS_SCHEDULED"),
        second.slug: _scoreboard(second, second_edition, status="STATUS_SCHEDULED"),
    }
    assert (
        execute(
            capture_options,
            repository=FakeRepository(),
            raw_store=raw_store,
            http_client=FakeHttpClient(raw_store, source_payloads),
        ).exit_code
        == 0
    )
    raw_bytes = Path(
        capture_options.raw_manifest_uri.removeprefix("file://")
    ).read_bytes()
    replay_source = {
        "mode": "repair",
        "run_id": capture_options.run_id,
        "attempt": capture_options.attempt,
        "plan_signature": capture_plan.signature(),
        "raw_manifest_sha256": hashlib.sha256(raw_bytes).hexdigest(),
    }
    replay_options, replay_plan = _plan(
        tmp_path / "replay",
        "replay",
        competitions,
        run_id="replay-run",
        replay_source=replay_source,
        selected_scopes=(first.scope_id(first_edition),),
    )
    replay_options, _ = _bind_replay_to_capture(
        replay_options, replay_plan, capture_options
    )
    repository = FakeRepository()
    result = execute(replay_options, repository=repository, raw_store=raw_store)
    assert result.exit_code == 0
    assert [item.plan.scope_id for item in repository.generations] == [
        first.scope_id(first_edition)
    ]


@pytest.mark.unit
def test_final_to_postponed_refresh_removes_stale_prior_summary_entities(tmp_path):
    competition, edition = _competition()
    prior = _prior_generation(competition, edition)
    scope_id = competition.scope_id(edition)
    as_of = date(2020, 9, 20)
    while is_full_reconciliation_day(scope_id, as_of):
        as_of += timedelta(days=1)
    options, _ = _plan(
        tmp_path,
        "daily",
        ((competition, edition),),
        as_of=as_of,
        initial_capture=False,
        priors={scope_id: prior},
    )
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    client = FakeHttpClient(
        raw_store,
        {
            competition.slug: _scoreboard(
                competition, edition, status="STATUS_POSTPONED"
            )
        },
    )
    repository = FakeRepository()
    assert (
        execute(
            options, repository=repository, raw_store=raw_store, http_client=client
        ).exit_code
        == 0
    )
    generation = repository.generations[0]
    assert generation.lineup == generation.matchsheet == generation.dispositions == ()
    assert all(item.endpoint != "summary" for item in generation.raw_ledger)


@pytest.mark.unit
def test_snapshot_conflict_is_scope_incomplete_and_other_scope_still_publishes(
    tmp_path,
):
    first, first_edition = _competition(730, "ita.1")
    second, second_edition = _competition(731, "eng.1")
    options, _ = _plan(
        tmp_path,
        "repair",
        ((first, first_edition), (second, second_edition)),
    )
    conflicting = tmp_path / "runs" / "run-1" / "attempt-1" / "scope-730:2020.json"
    conflicting.parent.mkdir(parents=True, exist_ok=True)
    conflicting.write_text('{"forged":true}', encoding="utf-8")
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    repository = FakeRepository()
    result = execute(
        options,
        repository=repository,
        raw_store=raw_store,
        http_client=FakeHttpClient(
            raw_store,
            {
                first.slug: _scoreboard(
                    first, first_edition, status="STATUS_SCHEDULED"
                ),
                second.slug: _scoreboard(
                    second, second_edition, status="STATUS_SCHEDULED"
                ),
            },
        ),
    )
    assert result.exit_code == 1
    assert [item.plan.scope_id for item in repository.generations] == [
        second.scope_id(second_edition)
    ]
    assert [item["state"] for item in result.payload["scopes"]] == [
        "incomplete",
        "complete",
    ]
    assert Path(options.output_uri.removeprefix("file://")).is_file()


@pytest.mark.unit
def test_snapshot_io_failure_happens_before_publish_and_retry_is_safe(
    tmp_path, monkeypatch
):
    import scrapers.espn.runner as runner_module

    competition, edition = _competition()
    options, _ = _plan(tmp_path, "backfill", ((competition, edition),))
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    snapshot_uri = (
        tmp_path / "runs" / "run-1" / "attempt-1" / "scope-730:2020.json"
    ).as_uri()
    real_write = runner_module._write_artifact
    failed = False

    def fail_snapshot_once(uri, payload, *, immutable):
        nonlocal failed
        if uri == snapshot_uri and not failed:
            failed = True
            raise OSError("injected durable snapshot failure")
        return real_write(uri, payload, immutable=immutable)

    monkeypatch.setattr(runner_module, "_write_artifact", fail_snapshot_once)
    first_repository = FakeRepository()
    first = execute(
        options,
        repository=first_repository,
        raw_store=raw_store,
        http_client=FakeHttpClient(
            raw_store, {competition.slug: _scoreboard(competition, edition)}
        ),
    )
    assert first.exit_code == 1
    assert first_repository.generations == []

    retry_client = FakeHttpClient(raw_store, {competition.slug: b"forbidden"})
    retry_repository = FakeRepository()
    retry = execute(
        options,
        repository=retry_repository,
        raw_store=raw_store,
        http_client=retry_client,
    )
    assert retry.exit_code == 0
    assert len(retry_repository.generations) == 1
    assert retry_client.calls == []


@pytest.mark.unit
def test_terminal_result_conflict_fails_same_identity(tmp_path):
    competition, edition = _competition()
    options, _ = _plan(tmp_path, "backfill", ((competition, edition),))
    raw_store = EspnRawStore.from_uri(options.raw_store_uri)
    client = FakeHttpClient(
        raw_store, {competition.slug: _scoreboard(competition, edition)}
    )
    repository = FakeRepository()
    assert (
        execute(
            options, repository=repository, raw_store=raw_store, http_client=client
        ).exit_code
        == 0
    )
    output_path = Path(options.output_uri.removeprefix("file://"))
    output_path.write_text('{"state":"complete"}', encoding="utf-8")
    with pytest.raises(ArtifactConflictError, match="terminal run result conflict"):
        execute(
            options,
            repository=FakeRepository(),
            raw_store=raw_store,
            http_client=FakeHttpClient(raw_store, {competition.slug: b"forbidden"}),
        )


@pytest.mark.unit
def test_replay_manifest_hash_run_and_attempt_are_exactly_bound(tmp_path):
    competition, edition = _competition()
    capture_options, capture_plan = _plan(
        tmp_path / "capture", "backfill", ((competition, edition),)
    )
    raw_store = EspnRawStore.from_uri(capture_options.raw_store_uri)
    assert (
        execute(
            capture_options,
            repository=FakeRepository(),
            raw_store=raw_store,
            http_client=FakeHttpClient(
                raw_store, {competition.slug: _scoreboard(competition, edition)}
            ),
        ).exit_code
        == 0
    )
    raw_path = Path(capture_options.raw_manifest_uri.removeprefix("file://"))
    original = raw_path.read_bytes()
    replay_source = {
        "mode": "backfill",
        "run_id": capture_options.run_id,
        "attempt": capture_options.attempt,
        "plan_signature": capture_plan.signature(),
        "raw_manifest_sha256": hashlib.sha256(original).hexdigest(),
    }
    replay_options, replay_plan = _plan(
        tmp_path / "replay",
        "replay",
        ((competition, edition),),
        run_id="replay-run",
        replay_source=replay_source,
    )
    replay_options, replay_plan = _bind_replay_to_capture(
        replay_options, replay_plan, capture_options
    )

    raw_path.write_bytes(original + b" ")
    with pytest.raises(RunnerConfigurationError, match="hash mismatch"):
        execute(replay_options, repository=FakeRepository(), raw_store=raw_store)

    raw_path.write_bytes(original)
    _reseal_raw_manifest(
        raw_path,
        lambda manifest: (
            manifest.__setitem__("run_id", "wrong-run"),
            manifest.__setitem__("attempt", 2),
        ),
    )
    drifted_bytes = raw_path.read_bytes()

    def bind_new_hash(document):
        source = document["metadata"]["runtime"]["replay_source"]
        source["raw_manifest_sha256"] = hashlib.sha256(drifted_bytes).hexdigest()

    _rewrite_signed_plan(replay_options, replay_plan, bind_new_hash)
    with pytest.raises(RunnerConfigurationError, match="identity mismatch"):
        execute(replay_options, repository=FakeRepository(), raw_store=raw_store)


@pytest.mark.unit
def test_native_import_path_never_imports_legacy_scraper_or_soccerdata():
    code = """
import builtins
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == 'scrapers.espn.scraper' or name.startswith('soccerdata'):
        raise AssertionError(name)
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded
import scrapers.espn.runner
import dags.scripts.run_espn_scraper
"""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path(__file__).resolve().parents[3],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
