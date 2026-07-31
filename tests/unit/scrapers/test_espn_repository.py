from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest

from scrapers.espn.models import (
    CapabilityState,
    DispositionState,
    EntityCapabilities,
    RequestDisposition,
    ScopePlan,
)
from scrapers.espn.parser_contracts import LineupRow, ScheduleRow
from scrapers.espn.repository import (
    CATALOG_TABLE,
    CUTOVER_TABLE,
    ENTITY_TABLES,
    MANIFEST_TABLE,
    PROVENANCE_COLUMNS,
    BatchPublicationResult,
    EspnBronzeRepository,
    ManifestConflictError,
    PublicationError,
    RawLedgerRecord,
    ScopeGeneration,
    ScopePublicationState,
    build_catalog_snapshot,
    render_current_view_sql,
    render_repository_ddl,
    row_fingerprint,
    validate_scope_generation,
)
from scrapers.espn.schedule_parser import parse_scoreboards
from scrapers.espn.summary_parser import parse_summary


FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "espn"
UTC = timezone.utc
SIG_A = "a" * 64
SIG_B = "b" * 64


def _raw(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _scope() -> ScopePlan:
    return ScopePlan(
        scope_id="730:2020",
        espn_id=730,
        slug="ita.1",
        source_season_year=2020,
        start_date=date(2020, 8, 1),
        end_date=date(2021, 7, 31),
        capabilities=EntityCapabilities(
            schedule=CapabilityState.PROVEN,
            lineup=CapabilityState.PROVEN,
            matchsheet=CapabilityState.PROVEN,
        ),
    )


def _parsed_rows():
    from scrapers.espn.models import AgeClass, Competition, Edition, Gender

    scope = _scope()
    edition = Edition(
        source_season_year=2020,
        display_name="2020/21",
        start_date=scope.start_date,
        end_date=scope.end_date,
        current=True,
        capabilities=scope.capabilities,
    )
    competition = Competition(
        espn_id=730,
        slug="ita.1",
        name="Test League",
        gender=Gender.MALE,
        age_class=AgeClass.SENIOR,
        enabled=True,
        editions=(edition,),
        gender_evidence=("fixture",),
        age_class_evidence=("manual",),
    )
    schedule = parse_scoreboards(
        [_raw("native_scoreboard.json")],
        competition=competition,
        edition=edition,
        query_start=scope.start_date,
        query_end=scope.end_date,
    )
    summary = parse_summary(
        _raw("native_summary.json"),
        competition=competition,
        edition=edition,
        event=schedule[0],
    )
    return schedule, summary.lineup, summary.matchsheet


def _ledger(event_id: int) -> tuple[RawLedgerRecord, ...]:
    return (
        RawLedgerRecord(
            request_id="scoreboard:20240817",
            endpoint="scoreboard",
            event_id=None,
            disposition=DispositionState.CAPTURED,
            raw_uri="s3://raw/scoreboard/a.json.gz",
            raw_sha256=SIG_A,
            fetched_at=datetime(2026, 7, 31, 8, tzinfo=UTC),
            direct_bytes=100,
            proxy_bytes=0,
            event_ids=(event_id,),
        ),
        RawLedgerRecord(
            request_id=f"summary:{event_id}",
            endpoint="summary",
            event_id=event_id,
            disposition=DispositionState.CAPTURED,
            raw_uri="s3://raw/summary/b.json.gz",
            raw_sha256=SIG_B,
            fetched_at=datetime(2026, 7, 31, 8, 1, tzinfo=UTC),
            direct_bytes=200,
            proxy_bytes=0,
        ),
    )


def _generation(**changes) -> ScopeGeneration:
    schedule, lineup, matchsheet = _parsed_rows()
    values = dict(
        plan=_scope(),
        run_id="run-1",
        generation_id="generation-1",
        registry_snapshot_uri="s3://raw/catalog/registry.json.gz",
        registry_signature=SIG_A,
        plan_signature=SIG_B,
        parser_version="espn-native-parser-v2",
        runtime_version="espn-native-runtime-v2",
        ingested_at=datetime(2026, 7, 31, 9, tzinfo=UTC),
        batch_id="batch-1",
        schedule=schedule,
        lineup=lineup,
        matchsheet=matchsheet,
        planned_request_ids=("scoreboard:20240817", f"summary:{schedule[0].event_id}"),
        raw_ledger=_ledger(schedule[0].event_id),
        dispositions=(
            RequestDisposition(
                endpoint="lineup",
                state=DispositionState.CAPTURED,
                detail="both sides",
                event_id=schedule[0].event_id,
            ),
            RequestDisposition(
                endpoint="matchsheet",
                state=DispositionState.CAPTURED,
                detail="both sides",
                event_id=schedule[0].event_id,
            ),
        ),
    )
    values.update(changes)
    return ScopeGeneration(**values)


class FakeWriter:
    def __init__(self, fail_table: str | None = None):
        self.fail_table = fail_table
        self.calls: list[tuple[str, pd.DataFrame]] = []

    def write_dataframe(self, df, *, database, table, **kwargs):
        self.calls.append((table, df.copy()))
        if table == self.fail_table:
            raise RuntimeError("injected write failure")
        return f"iceberg.{database}.{table}"


class FakeQuery:
    def __init__(self):
        self.calls: list[tuple[str, tuple | None]] = []
        self.manifests: dict[tuple[str, str], tuple] = {}
        self.physical: dict[tuple[str, str], tuple[int, str]] = {}

    def execute_query(self, sql, params=None):
        self.calls.append((sql, params))
        if "FROM iceberg.bronze.espn_ingest_manifest_v2" in sql:
            row = self.manifests.get((params[0], params[1]))
            return [row] if row else []
        if " AS entity" in sql and "UNION ALL" in sql:
            return [
                (entity, *self.physical[(params[index], entity)])
                for index, entity in enumerate(ENTITY_TABLES)
            ]
        return []


class PhysicalQuery(FakeQuery):
    def __init__(self, generation: ScopeGeneration, existing=None):
        super().__init__()
        self.report = validate_scope_generation(generation)
        self.existing = existing or {}

    def execute_query(self, sql, params=None):
        self.calls.append((sql, params))
        if "FROM iceberg.bronze.espn_ingest_manifest_v2" in sql:
            return []
        if 'SELECT DISTINCT "_row_sha256"' in sql:
            entity = next(
                entity for entity, table in ENTITY_TABLES.items() if table in sql
            )
            return [(value,) for value in self.existing.get(entity, ())]
        if " AS entity" in sql and "UNION ALL" in sql:
            return [
                (
                    entity,
                    self.report.row_counts[entity],
                    self.report.row_hashes[entity],
                )
                for entity in ENTITY_TABLES
            ]
        return []


@pytest.mark.unit
def test_ddl_and_views_have_append_only_contract_and_full_join_identity():
    ddl = render_repository_ddl()
    assert set(ENTITY_TABLES.values()) | {
        CATALOG_TABLE,
        MANIFEST_TABLE,
        CUTOVER_TABLE,
    } <= set(ddl)
    assert all("CREATE TABLE IF NOT EXISTS" in ddl[name] for name in ddl)
    for entity in ENTITY_TABLES:
        sql = render_current_view_sql(entity)
        assert "status = 'complete'" in sql
        assert "ROW_NUMBER() OVER" in sql
        assert "completed_at DESC, generation_id DESC, manifest_sha256 DESC" in sql
        for key in (
            "scope_id",
            "competition_id",
            "source_season_year",
            "generation_id",
            "run_id",
            "_batch_id",
            "registry_snapshot_uri",
            "registry_signature",
            "plan_signature",
            "parser_version",
            "runtime_version",
        ):
            assert f'g."{key}" = m."{key}"' in sql
        assert "espn_scope_cutover_v2" in sql
        assert "active_source = 'native'" in sql
        assert "active_source = 'legacy'" in sql
        assert "NOT EXISTS" in sql
        assert "physical_fence" in sql
        assert f"'$.{entity}'" in sql


@pytest.mark.unit
def test_generation_tables_cover_legacy_and_provenance_columns():
    schema = json.loads(
        (
            Path(__file__).resolve().parents[2] / "fixtures" / "bronze_schemas.json"
        ).read_text()
    )["tables"]
    ddl = render_repository_ddl()
    for entity, physical in ENTITY_TABLES.items():
        legacy = set(schema[f"bronze.espn_{entity}"]["columns"])
        assert all(
            f'"{column}"' in ddl[physical]
            for column in legacy | set(PROVENANCE_COLUMNS)
        )


@pytest.mark.unit
def test_scope_generation_is_frozen_strict_and_deterministic():
    generation = _generation()
    with pytest.raises(Exception):
        generation.run_id = "changed"
    assert generation.manifest_sha256 == _generation().manifest_sha256
    with pytest.raises((TypeError, ValueError), match="competition_id"):
        ScopeGeneration(
            **{
                **generation.constructor_values(),
                "schedule": (
                    {**asdict(generation.schedule[0]), "competition_id": True},
                ),
            }
        )


@pytest.mark.unit
def test_exact_scope_dq_accepts_parsed_fixture_and_binds_hashes():
    report = validate_scope_generation(_generation())
    assert report.passed
    assert report.row_counts == {"schedule": 1, "lineup": 2, "matchsheet": 2}
    assert set(report.row_hashes) == set(ENTITY_TABLES)
    assert all(len(value) == 64 for value in report.row_hashes.values())


@pytest.mark.unit
@pytest.mark.parametrize(
    ("mutator", "failure"),
    [
        (
            lambda g: {"planned_request_ids": ("scoreboard:missing",)},
            "planned/raw ledger parity",
        ),
        (
            lambda g: {
                "raw_ledger": (
                    *g.raw_ledger[:-1],
                    RawLedgerRecord(
                        **{**g.raw_ledger[-1].constructor_values(), "proxy_bytes": 1}
                    ),
                )
            },
            "proxy bytes",
        ),
        (
            lambda g: {
                "schedule": (
                    ScheduleRow(
                        **{
                            **asdict(g.schedule[0]),
                            "kickoff": datetime(2027, 1, 1, tzinfo=UTC),
                        }
                    ),
                )
            },
            "edition window",
        ),
        (
            lambda g: {"schedule": (g.schedule[0], g.schedule[0])},
            "schedule event uniqueness",
        ),
        (lambda g: {"lineup": (*g.lineup, g.lineup[0])}, "lineup natural key"),
        (
            lambda g: {
                "lineup": (LineupRow(**{**asdict(g.lineup[0]), "event_id": 999}),)
            },
            "entity schedule FK",
        ),
        (lambda g: {"matchsheet": (g.matchsheet[0],)}, "matchsheet two-side"),
        (lambda g: {"dispositions": ()}, "played-final disposition"),
    ],
)
def test_dq_negative_branches_fail_closed(mutator, failure):
    generation = _generation()
    report = validate_scope_generation(
        ScopeGeneration(**{**generation.constructor_values(), **mutator(generation)})
    )
    assert not report.passed
    assert any(failure in item for item in report.failures)


@pytest.mark.unit
def test_terminal_nonplayed_does_not_require_summary_entities():
    generation = _generation()
    schedule = ScheduleRow(
        **{
            **asdict(generation.schedule[0]),
            "status": "cancelled",
            "played_final": False,
            "terminal_nonplayed": True,
            "summary_required": False,
            "home_score": None,
            "away_score": None,
        }
    )
    candidate = ScopeGeneration(
        **{
            **generation.constructor_values(),
            "schedule": (schedule,),
            "lineup": (),
            "matchsheet": (),
            "dispositions": (),
        }
    )
    assert validate_scope_generation(candidate).passed


@pytest.mark.unit
def test_valid_empty_requires_capability_and_successful_raw():
    generation = _generation()
    partial = ScopePlan(
        **{
            **generation.plan.to_dict(),
            "start_date": generation.plan.start_date,
            "end_date": generation.plan.end_date,
            "capabilities": EntityCapabilities(
                schedule=CapabilityState.PROVEN,
                lineup=CapabilityState.PARTIAL,
                matchsheet=CapabilityState.PARTIAL,
            ),
        }
    )
    empty_dispositions = tuple(
        RequestDisposition(
            endpoint=entity,
            state=DispositionState.VALID_EMPTY,
            detail="section absent in successful Summary",
            event_id=generation.schedule[0].event_id,
        )
        for entity in ("lineup", "matchsheet")
    )
    candidate = ScopeGeneration(
        **{
            **generation.constructor_values(),
            "plan": partial,
            "lineup": (),
            "matchsheet": (),
            "dispositions": empty_dispositions,
        }
    )
    assert validate_scope_generation(candidate).passed
    failed_raw = RawLedgerRecord(
        **{
            **candidate.raw_ledger[-1].constructor_values(),
            "disposition": DispositionState.FAILED,
            "raw_uri": None,
            "raw_sha256": None,
            "fetched_at": None,
        }
    )
    bad = ScopeGeneration(
        **{
            **candidate.constructor_values(),
            "raw_ledger": (*candidate.raw_ledger[:-1], failed_raw),
        }
    )
    assert not validate_scope_generation(bad).passed


@pytest.mark.unit
def test_schedule_requires_exact_row_to_scoreboard_raw_binding():
    generation = _generation()
    unbound = RawLedgerRecord(
        **{**generation.raw_ledger[0].constructor_values(), "event_ids": ()}
    )
    candidate = ScopeGeneration(
        **{
            **generation.constructor_values(),
            "raw_ledger": (unbound, *generation.raw_ledger[1:]),
        }
    )
    report = validate_scope_generation(candidate)
    assert not report.passed
    assert "schedule raw binding must be exact" in report.failures[0]


@pytest.mark.unit
def test_summary_requires_one_captured_raw_binding_per_event():
    generation = _generation()
    duplicate = RawLedgerRecord(
        **{
            **generation.raw_ledger[-1].constructor_values(),
            "request_id": f"summary-duplicate:{generation.schedule[0].event_id}",
        }
    )
    candidate = ScopeGeneration(
        **{
            **generation.constructor_values(),
            "planned_request_ids": (
                *generation.planned_request_ids,
                duplicate.request_id,
            ),
            "raw_ledger": (*generation.raw_ledger, duplicate),
        }
    )
    report = validate_scope_generation(candidate)
    assert not report.passed
    assert "Summary raw binding must be exact per event" in report.failures


@pytest.mark.unit
def test_physical_verification_handles_valid_empty_with_sha256_empty():
    generation = _generation()
    partial_plan = ScopePlan(
        scope_id=generation.plan.scope_id,
        espn_id=generation.plan.espn_id,
        slug=generation.plan.slug,
        source_season_year=generation.plan.source_season_year,
        start_date=generation.plan.start_date,
        end_date=generation.plan.end_date,
        capabilities=EntityCapabilities(
            schedule=CapabilityState.PROVEN,
            lineup=CapabilityState.PARTIAL,
            matchsheet=CapabilityState.PARTIAL,
        ),
    )
    empty = ScopeGeneration(
        **{
            **generation.constructor_values(),
            "plan": partial_plan,
            "lineup": (),
            "matchsheet": (),
            "dispositions": tuple(
                RequestDisposition(
                    endpoint=entity,
                    state=DispositionState.VALID_EMPTY,
                    detail="successful absent section",
                    event_id=generation.schedule[0].event_id,
                )
                for entity in ("lineup", "matchsheet")
            ),
        }
    )
    writer = FakeWriter()
    query = PhysicalQuery(empty)
    result = EspnBronzeRepository(
        writer=writer, query=query, verify_physical=True
    ).publish_scope(empty)
    assert result.state is ScopePublicationState.PUBLISHED
    parity_sql = next(sql for sql, _ in query.calls if "UNION ALL" in sql)
    assert "COALESCE" in parity_sql
    assert "sha256(to_utf8(''))" in parity_sql


@pytest.mark.unit
def test_partial_generation_retry_appends_only_missing_hashes_then_manifest():
    generation = _generation()
    existing = {
        "schedule": {row_fingerprint(generation, "schedule", generation.schedule[0])},
        "lineup": {row_fingerprint(generation, "lineup", generation.lineup[0])},
    }
    writer = FakeWriter()
    query = PhysicalQuery(generation, existing=existing)
    result = EspnBronzeRepository(
        writer=writer, query=query, verify_physical=True
    ).publish_scope(generation)
    assert result.state is ScopePublicationState.PUBLISHED
    assert [table for table, _ in writer.calls] == [
        ENTITY_TABLES["lineup"],
        ENTITY_TABLES["matchsheet"],
        MANIFEST_TABLE,
    ]
    assert len(writer.calls[0][1]) == len(generation.lineup) - 1


@pytest.mark.unit
def test_partial_retry_with_changed_raw_provenance_fails_as_conflict():
    original = _generation()
    existing = {
        entity: {
            row_fingerprint(original, entity, row) for row in getattr(original, entity)
        }
        for entity in ENTITY_TABLES
    }
    changed_summary = RawLedgerRecord(
        **{
            **original.raw_ledger[-1].constructor_values(),
            "raw_uri": "s3://raw/summary/changed.json.gz",
            "raw_sha256": "c" * 64,
        }
    )
    changed = ScopeGeneration(
        **{
            **original.constructor_values(),
            "raw_ledger": (*original.raw_ledger[:-1], changed_summary),
        }
    )
    writer = FakeWriter()
    repository = EspnBronzeRepository(
        writer=writer,
        query=PhysicalQuery(changed, existing=existing),
        verify_physical=True,
    )
    with pytest.raises(ManifestConflictError, match="conflicting physical rows"):
        repository.publish_scope(changed)
    assert not writer.calls


@pytest.mark.unit
def test_manifest_is_last_and_partial_commit_is_not_published():
    writer = FakeWriter(fail_table=ENTITY_TABLES["matchsheet"])
    repository = EspnBronzeRepository(
        writer=writer, query=FakeQuery(), verify_physical=False
    )
    with pytest.raises(PublicationError):
        repository.publish_scope(_generation())
    assert [table for table, _ in writer.calls] == [
        ENTITY_TABLES["schedule"],
        ENTITY_TABLES["lineup"],
        ENTITY_TABLES["matchsheet"],
    ]
    assert MANIFEST_TABLE not in [table for table, _ in writer.calls]


@pytest.mark.unit
def test_success_writes_all_entities_then_manifest_and_replay_is_idempotent():
    writer = FakeWriter()
    query = FakeQuery()
    repository = EspnBronzeRepository(writer=writer, query=query, verify_physical=False)
    generation = _generation()
    result = repository.publish_scope(generation)
    assert result.state is ScopePublicationState.PUBLISHED
    assert [table for table, _ in writer.calls] == [
        *ENTITY_TABLES.values(),
        MANIFEST_TABLE,
    ]
    manifest_row = writer.calls[-1][1].iloc[0].to_dict()
    query.manifests[(generation.plan.scope_id, generation.generation_id)] = tuple(
        manifest_row[column] for column in repository.manifest_columns
    )
    before = len(writer.calls)
    replay = repository.publish_scope(generation)
    assert replay.state is ScopePublicationState.IDEMPOTENT
    assert len(writer.calls) == before


@pytest.mark.unit
def test_conflicting_same_manifest_identity_fails_closed():
    writer = FakeWriter()
    query = FakeQuery()
    repository = EspnBronzeRepository(writer=writer, query=query, verify_physical=False)
    generation = _generation()
    query.manifests[(generation.plan.scope_id, generation.generation_id)] = tuple(
        "different"
        if column == "manifest_sha256"
        else generation.manifest_row()[column]
        for column in repository.manifest_columns
    )
    with pytest.raises(ManifestConflictError):
        repository.publish_scope(generation)
    assert not writer.calls


@pytest.mark.unit
def test_two_scope_batch_publishes_good_scope_and_returns_failed_verdict():
    good = _generation()
    bad_plan = ScopePlan(
        scope_id="701:2024",
        espn_id=701,
        slug="bad.1",
        source_season_year=2024,
        start_date=date(2024, 8, 1),
        end_date=date(2025, 6, 30),
        capabilities=good.plan.capabilities,
    )
    bad_schedule = tuple(
        ScheduleRow(
            **{
                **asdict(row),
                "scope_id": bad_plan.scope_id,
                "competition_id": 701,
                "competition_slug": bad_plan.slug,
                "source_season_year": 2024,
                "kickoff": datetime(2027, 1, 1, tzinfo=UTC),
            }
        )
        for row in good.schedule
    )
    bad = ScopeGeneration(
        **{
            **good.constructor_values(),
            "plan": bad_plan,
            "generation_id": "generation-bad",
            "batch_id": "batch-bad",
            "schedule": bad_schedule,
            "lineup": (),
            "matchsheet": (),
        }
    )
    writer = FakeWriter()
    repository = EspnBronzeRepository(
        writer=writer, query=FakeQuery(), verify_physical=False
    )
    verdict = repository.publish_many((good, bad))
    assert isinstance(verdict, BatchPublicationResult)
    assert not verdict.passed
    assert verdict.results[0].state is ScopePublicationState.PUBLISHED
    assert verdict.results[1].state is ScopePublicationState.FAILED
    manifests = writer.calls[-1][1]
    assert manifests.iloc[0]["scope_id"] == good.plan.scope_id


@pytest.mark.unit
def test_catalog_snapshot_and_cutover_preserve_rollback_metadata():
    captured_at = datetime(2026, 7, 31, 7, tzinfo=UTC)
    rows = build_catalog_snapshot(
        snapshot_id="registry-20260731",
        registry_signature=SIG_A,
        captured_at=captured_at,
        run_id="discovery-1",
        raw_uri="s3://raw/catalog/a.json.gz",
        raw_sha256=SIG_B,
        parser_version="espn-native-parser-v2",
        runtime_version="espn-native-runtime-v2",
        ingested_at=captured_at,
        batch_id="catalog-batch-1",
        competitions=({"espn_id": 700, "slug": "eng.1"},),
    )
    assert rows[0]["registry_signature"] == SIG_A
    assert rows[0]["raw_sha256"] == SIG_B
    cutover = render_repository_ddl()[CUTOVER_TABLE]
    for column in (
        "active_source",
        "previous_source",
        "rollback_run_id",
        "rollback_reason",
        "legacy_league",
        "legacy_season",
    ):
        assert f'"{column}"' in cutover
