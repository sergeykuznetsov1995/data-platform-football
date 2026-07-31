from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
import json
from pathlib import Path

import pandas as pd
import pytest

import scrapers.espn.repository as repository_module
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
    canonical_json,
    render_current_view_sql,
    render_repository_ddl,
    row_fingerprint,
    validate_scope_generation,
)
from scrapers.espn.operations import LeaseLost
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
        self.options: list[dict] = []

    def write_dataframe(self, df, *, database, table, **kwargs):
        self.calls.append((table, df.copy()))
        self.options.append({"database": database, **kwargs})
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
        if (
            sql.lstrip().startswith("SELECT")
            and "FROM iceberg.bronze.espn_ingest_manifest_v2" in sql
        ):
            row = self.manifests.get((params[0], params[1]))
            return [row] if row else []
        if " AS entity" in sql and "UNION ALL" in sql:
            return [
                (entity, *self.physical[(params[index], entity)])
                for index, entity in enumerate(ENTITY_TABLES)
            ]
        return []


class PhysicalQuery(FakeQuery):
    def __init__(
        self,
        generation: ScopeGeneration,
        existing=None,
        existing_signatures=None,
    ):
        super().__init__()
        self.generation = generation
        self.report = validate_scope_generation(generation)
        self.existing = existing or {}
        self.existing_signatures = existing_signatures or {}

    def execute_query(self, sql, params=None):
        self.calls.append((sql, params))
        if (
            sql.lstrip().startswith("SELECT")
            and "FROM iceberg.bronze.espn_ingest_manifest_v2" in sql
        ):
            row = self.manifests.get((params[0], params[1]))
            return [row] if row else []
        if 'SELECT DISTINCT "generation_signature", "_row_sha256"' in sql:
            relation_tables = {
                **ENTITY_TABLES,
                "ledger": repository_module.LEDGER_TABLE,
            }
            entity = next(
                entity for entity, table in relation_tables.items() if table in sql
            )
            return [
                (
                    self.existing_signatures.get(
                        entity, self.generation.generation_signature
                    ),
                    value,
                )
                for value in self.existing.get(entity, ())
            ]
        if " AS entity" in sql and "UNION ALL" in sql:
            entities = [
                (
                    entity,
                    self.report.row_counts[entity],
                    self.report.row_hashes[entity],
                )
                for entity in ENTITY_TABLES
            ]
            return [
                *entities,
                ("ledger", self.report.ledger_count, self.report.ledger_hash),
            ]
        return []


class RepositoryStateQuery(FakeQuery):
    def __init__(self):
        super().__init__()
        self.catalog_rows: list[tuple[int, str, str]] = []
        self.cutover_hashes: dict[str, list[str]] = {}
        self.cutover_slots: dict[tuple[str, str | None], list[tuple[str, str]]] = {}
        self.latest_cutovers: dict[str, tuple[str, str, str, datetime, str, str]] = {}
        self.unresolved_cutover_forks: set[str] = set()

    def execute_query(self, sql, params=None):
        if "unresolved_cutover_forks" in sql:
            self.calls.append((sql, params))
            return (
                [("f" * 64,)] if str(params[0]) in self.unresolved_cutover_forks else []
            )
        if f"FROM iceberg.bronze.{CATALOG_TABLE}" in sql:
            self.calls.append((sql, params))
            return list(self.catalog_rows)
        if (
            f"FROM iceberg.bronze.{CUTOVER_TABLE}" in sql
            and 'WHERE "cutover_id" = ?' in sql
        ):
            self.calls.append((sql, params))
            return [(value,) for value in self.cutover_hashes.get(str(params[0]), [])]
        if (
            f"FROM iceberg.bronze.{CUTOVER_TABLE}" in sql
            and 'WHERE "scope_id" = ?' in sql
            and '"predecessor_cutover_sha256" IS NOT DISTINCT FROM ?' in sql
        ):
            self.calls.append((sql, params))
            return list(self.cutover_slots.get((str(params[0]), params[1]), []))
        if (
            f"FROM iceberg.bronze.{CUTOVER_TABLE}" in sql
            and 'WHERE "scope_id" = ?' in sql
        ):
            self.calls.append((sql, params))
            latest = self.latest_cutovers.get(str(params[0]))
            return [latest] if latest else []
        return super().execute_query(sql, params=params)


CUTOVER_GRAPH_COLUMNS = (
    "cutover_id",
    "scope_id",
    "cutover_sha256",
    "predecessor_cutover_id",
    "predecessor_cutover_sha256",
    "ancestor_cutover_sha256_json",
    "ancestor_lineage_sha256",
)


class CutoverGraphQuery(FakeQuery):
    def __init__(self, rows):
        super().__init__()
        self.rows = list(rows)

    def execute_query(self, sql, params=None):
        if "cutover_ancestry_rollout_gate" in sql:
            self.calls.append((sql, params))
            if 'WHERE "ancestor_cutover_sha256_json" IS NULL' in sql:
                return [row for row in self.rows if row[5] is None or row[6] is None][
                    :1
                ]
            return list(self.rows)
        return super().execute_query(sql, params=params)


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
            "generation_signature",
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
        assert "NOT EXISTS" in sql
        assert "validated_complete" in sql
        assert "native_ready" in sql
        assert f"'$.{entity}'" in sql


@pytest.mark.unit
def test_ensure_objects_evolves_cutover_ancestry_before_creating_views():
    query = FakeQuery()
    repository = EspnBronzeRepository(writer=FakeWriter(), query=query)

    repository.ensure_objects()

    statements = [sql for sql, _ in query.calls]
    alterations = [
        index
        for index, sql in enumerate(statements)
        if f"ALTER TABLE iceberg.bronze.{CUTOVER_TABLE}" in sql
    ]
    gate = next(
        index
        for index, sql in enumerate(statements)
        if "cutover_ancestry_rollout_gate" in sql
    )
    first_view = next(
        index
        for index, sql in enumerate(statements)
        if sql.startswith("CREATE OR REPLACE VIEW")
    )
    assert len(alterations) == 2
    assert max(alterations) < gate < first_view
    assert all("ADD COLUMN IF NOT EXISTS" in statements[index] for index in alterations)


@pytest.mark.unit
def test_ensure_objects_blocks_legacy_cutovers_until_ancestry_is_migrated():
    class LegacyCutoverQuery(FakeQuery):
        def execute_query(self, sql, params=None):
            if "cutover_ancestry_rollout_gate" in sql:
                self.calls.append((sql, params))
                return [("legacy-cutover",)]
            return super().execute_query(sql, params=params)

    query = LegacyCutoverQuery()
    repository = EspnBronzeRepository(writer=FakeWriter(), query=query)

    with pytest.raises(PublicationError, match="ancestry migration"):
        repository.ensure_objects()
    assert not any(sql.startswith("CREATE OR REPLACE VIEW") for sql, _ in query.calls)


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
            "planned_request_ids": (generation.planned_request_ids[0],),
            "raw_ledger": (generation.raw_ledger[0],),
            "dispositions": (),
        }
    )
    assert validate_scope_generation(candidate).passed


@pytest.mark.unit
@pytest.mark.parametrize(
    ("status", "home_score", "away_score"),
    [
        ("STATUS_FORFEIT", 3, 0),
        ("STATUS_FORFEIT", None, None),
        ("STATUS_WALKOVER", 3, 0),
        ("STATUS_WALKOVER", None, None),
        ("STATUS_CANCELED", None, None),
        ("STATUS_ABANDONED", 1, 0),
        ("STATUS_ABANDONED", 1, None),
    ],
)
def test_terminal_nonplayed_preserves_source_administrative_or_partial_scores(
    status, home_score, away_score
):
    generation = _generation()
    schedule = ScheduleRow(
        **{
            **asdict(generation.schedule[0]),
            "status": status,
            "terminal": True,
            "played_final": False,
            "terminal_nonplayed": True,
            "summary_required": False,
            "home_score": home_score,
            "away_score": away_score,
            "home_goals": None if home_score is None else str(home_score),
            "away_goals": None if away_score is None else str(away_score),
        }
    )
    candidate = ScopeGeneration(
        **{
            **generation.constructor_values(),
            "schedule": (schedule,),
            "lineup": (),
            "matchsheet": (),
            "planned_request_ids": (generation.planned_request_ids[0],),
            "raw_ledger": (generation.raw_ledger[0],),
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
    assert "schedule raw binding must be exact" in " ".join(report.failures)


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
        repository_module.LEDGER_TABLE,
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
def test_abandoned_generation_with_different_signature_blocks_identity_reuse():
    original = _generation()
    changed_raw = RawLedgerRecord(
        **{
            **original.raw_ledger[-1].constructor_values(),
            "raw_uri": "s3://raw/summary/concurrent.json.gz",
            "raw_sha256": "c" * 64,
        }
    )
    changed = ScopeGeneration(
        **{
            **original.constructor_values(),
            "raw_ledger": (*original.raw_ledger[:-1], changed_raw),
        }
    )
    existing = {
        "schedule": {row_fingerprint(original, "schedule", original.schedule[0])}
    }
    query = PhysicalQuery(
        changed,
        existing=existing,
        existing_signatures={"schedule": original.generation_signature},
    )
    writer = FakeWriter()
    repository = EspnBronzeRepository(
        writer=writer,
        query=query,
        ensure_objects_on_write=False,
    )
    with pytest.raises(ManifestConflictError, match="content signature"):
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
def test_reclaimed_lease_is_rechecked_before_complete_manifest():
    """A writer fenced after physical rows must never expose COMPLETE."""

    writer = FakeWriter()
    repository = EspnBronzeRepository(
        writer=writer,
        query=FakeQuery(),
        verify_physical=False,
        ensure_objects_on_write=False,
    )

    def publication_fence():
        written = {table for table, _ in writer.calls}
        if repository_module.LEDGER_TABLE in written:
            raise LeaseLost("lease reclaimed")

    with pytest.raises(LeaseLost, match="reclaimed"):
        repository.publish_scope(_generation(), publication_fence=publication_fence)

    assert repository_module.LEDGER_TABLE in {table for table, _ in writer.calls}
    assert MANIFEST_TABLE not in {table for table, _ in writer.calls}


@pytest.mark.unit
def test_success_writes_all_entities_then_manifest_and_replay_is_idempotent():
    writer = FakeWriter()
    query = FakeQuery()
    repository = EspnBronzeRepository(writer=writer, query=query, verify_physical=False)
    generation = _generation()
    assert repository.exact_complete_exists(generation) is False
    result = repository.publish_scope(generation)
    assert result.state is ScopePublicationState.PUBLISHED
    assert [table for table, _ in writer.calls] == [
        *ENTITY_TABLES.values(),
        repository_module.LEDGER_TABLE,
        MANIFEST_TABLE,
    ]
    manifest_row = writer.calls[-1][1].iloc[0].to_dict()
    query.manifests[(generation.plan.scope_id, generation.generation_id)] = tuple(
        manifest_row[column] for column in repository.manifest_columns
    )
    assert repository.exact_complete_exists(generation) is True
    before = len(writer.calls)
    replay = repository.publish_scope(generation)
    assert replay.state is ScopePublicationState.IDEMPOTENT
    assert len(writer.calls) == before


@pytest.mark.unit
def test_published_dq_verifies_exact_manifest_and_rows_without_an_append():
    generation = _generation()
    report = validate_scope_generation(generation)
    query = PhysicalQuery(generation)
    manifest = generation.manifest_row(report)
    query.manifests[(generation.plan.scope_id, generation.generation_id)] = tuple(
        manifest[column] for column in EspnBronzeRepository.manifest_columns
    )
    writer = FakeWriter()
    repository = EspnBronzeRepository(writer=writer, query=query)

    observed = repository.verify_published_scope(generation)

    assert observed == report
    assert writer.calls == []


@pytest.mark.unit
def test_published_dq_rejects_noncomplete_exact_generation_manifest():
    generation = _generation()
    report = validate_scope_generation(generation)
    manifest = generation.manifest_row(report)
    query = PhysicalQuery(generation)
    query.manifests[(generation.plan.scope_id, generation.generation_id)] = tuple(
        "incomplete" if column == "status" else manifest[column]
        for column in EspnBronzeRepository.manifest_columns
    )
    repository = EspnBronzeRepository(writer=FakeWriter(), query=query)

    with pytest.raises(ManifestConflictError, match="status"):
        repository.verify_published_scope(generation)


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
    snapshot = build_catalog_snapshot(
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
    assert snapshot.rows[0]["registry_signature"] == SIG_A
    assert snapshot.rows[0]["raw_sha256"] == SIG_B
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


# Fix-round regressions: these describe the scope-level commit boundary, not
# implementation details of the in-memory fakes above.


@pytest.mark.unit
def test_current_view_validates_all_four_relations_before_ranking_and_falls_back():
    sql = render_current_view_sql("schedule")
    assert repository_module.LEDGER_TABLE in sql
    assert "generation_signature" in sql
    assert "conflicting_complete_identities" in sql
    assert "validated_complete" in sql
    assert sql.index("validated_complete") < sql.index("ranked_manifests")
    assert "LEFT JOIN schedule_fence" in sql
    assert "LEFT JOIN lineup_fence" in sql
    assert "LEFT JOIN matchsheet_fence" in sql
    assert "LEFT JOIN ledger_fence" in sql
    assert sql.count("COALESCE(") >= 8
    assert "native_ready" in sql
    assert "JOIN latest_validated" in sql
    assert "FROM validated_complete ready_manifest" in sql
    assert "AND EXISTS (" in sql
    assert "TRY_CAST(TRY(json_extract_scalar" in sql
    assert "TRY(json_extract_scalar(m.row_hashes_json" in sql
    # The cutover generation is activation evidence, not a version pin.
    native_rows = sql[sql.index("native_rows AS") : sql.index("legacy_rows AS")]
    assert "JOIN latest_validated m" in native_rows
    assert "JOIN native_ready c ON c.scope_id = m.scope_id" in native_rows
    assert "c.native_generation_id" not in native_rows


@pytest.mark.unit
def test_current_view_conflict_and_zero_count_contract_is_manifest_led():
    sql = render_current_view_sql("lineup")
    assert "COUNT(DISTINCT manifest_sha256)" in sql
    assert "HAVING COUNT(DISTINCT" in sql
    assert "sha256(to_utf8(''))" in sql
    assert "COALESCE(lineup_fence.row_count, 0)" in sql
    assert "COALESCE(lineup_fence.row_hash" in sql
    # A cutover row alone cannot suppress the legacy fallback.
    legacy_block = sql[sql.index("legacy_rows AS") :]
    assert "native_ready" in legacy_block
    assert "native_scopes" not in legacy_block


@pytest.mark.unit
def test_current_view_signature_isolates_unmanifested_concurrent_attempts():
    sql = render_current_view_sql("matchsheet")
    for relation in (*ENTITY_TABLES, "ledger"):
        assert (
            f'{relation}_fence."generation_signature" = m."generation_signature"'
        ) in sql
    assert "physical_generation_signatures" not in sql


@pytest.mark.unit
def test_current_view_bounds_every_physical_fence_to_complete_candidates():
    sql = render_current_view_sql("schedule")
    assert "candidate_generation_identities AS" in sql
    assert "SELECT DISTINCT" in sql[sql.index("candidate_generation_identities AS") :]
    for relation in (*ENTITY_TABLES, "ledger"):
        block = sql[
            sql.index(f"ranked_{relation}_rows AS") : sql.index(
                f"), {relation}_rows AS"
            )
        ]
        assert "JOIN candidate_generation_identities candidate" in block
        for key in (
            "scope_id",
            "competition_id",
            "source_season_year",
            "generation_id",
            "generation_signature",
            "run_id",
            "_batch_id",
            "registry_snapshot_uri",
            "registry_signature",
            "plan_signature",
            "parser_version",
            "runtime_version",
        ):
            assert f'r."{key}" = candidate."{key}"' in block

    candidate = (
        "730:2020",
        730,
        2020,
        "generation-ready",
        "a" * 64,
        "run-ready",
        "batch-ready",
        "s3://raw/catalog/registry.json.gz",
        "b" * 64,
        "c" * 64,
        "parser-v2",
        "runtime-v2",
    )
    orphan = (*candidate[:-1], "runtime-unmanifested")
    candidates = {candidate}
    physical = {
        (*candidate, "ready-row"),
        (*orphan, "orphan-row"),
    }
    bounded = {row for row in physical if row[:-1] in candidates}
    assert bounded == {(*candidate, "ready-row")}


@pytest.mark.unit
def test_generation_signature_changes_with_raw_or_runtime_and_is_in_every_row():
    generation = _generation()
    changed_raw = RawLedgerRecord(
        **{
            **generation.raw_ledger[-1].constructor_values(),
            "raw_uri": "s3://raw/summary/changed-v2.json.gz",
            "raw_sha256": "c" * 64,
        }
    )
    changed = ScopeGeneration(
        **{
            **generation.constructor_values(),
            "raw_ledger": (*generation.raw_ledger[:-1], changed_raw),
        }
    )
    assert generation.generation_signature != changed.generation_signature
    assert (
        generation.manifest_row()["generation_signature"]
        == generation.generation_signature
    )
    ddl = render_repository_ddl()
    for table in (
        *ENTITY_TABLES.values(),
        repository_module.LEDGER_TABLE,
        MANIFEST_TABLE,
    ):
        assert '"generation_signature"' in ddl[table]


@pytest.mark.unit
def test_dq_rejects_noncaptured_planned_raw_and_scoreboard_binding_drift():
    generation = _generation()
    failed = RawLedgerRecord(
        **{
            **generation.raw_ledger[-1].constructor_values(),
            "disposition": DispositionState.FAILED,
            "raw_uri": None,
            "raw_sha256": None,
            "fetched_at": None,
        }
    )
    failed_generation = ScopeGeneration(
        **{
            **generation.constructor_values(),
            "raw_ledger": (*generation.raw_ledger[:-1], failed),
        }
    )
    assert "planned raw request is not captured" in " ".join(
        validate_scope_generation(failed_generation).failures
    )

    contaminated = RawLedgerRecord(
        **{
            **generation.raw_ledger[0].constructor_values(),
            "event_ids": (*generation.raw_ledger[0].event_ids, 999999999),
        }
    )
    contaminated_generation = ScopeGeneration(
        **{
            **generation.constructor_values(),
            "raw_ledger": (contaminated, *generation.raw_ledger[1:]),
        }
    )
    assert "scoreboard event binding parity" in " ".join(
        validate_scope_generation(contaminated_generation).failures
    )


@pytest.mark.unit
def test_dq_validates_every_disposition_and_every_emitted_group():
    generation = _generation()
    nonfinal = ScheduleRow(
        **{
            **asdict(generation.schedule[0]),
            "terminal": False,
            "played_final": False,
            "summary_required": False,
            "home_score": None,
            "away_score": None,
            "home_goals": None,
            "away_goals": None,
        }
    )
    invalid_empty = ScopeGeneration(
        **{
            **generation.constructor_values(),
            "schedule": (nonfinal,),
            "lineup": (),
            "matchsheet": (),
            "dispositions": (
                RequestDisposition(
                    endpoint="lineup",
                    state=DispositionState.VALID_EMPTY,
                    detail="not allowed for proven",
                    event_id=nonfinal.event_id,
                ),
            ),
        }
    )
    report = validate_scope_generation(invalid_empty)
    assert not report.passed
    assert "valid_empty is forbidden for proven lineup" in report.failures

    one_side = ScopeGeneration(
        **{
            **generation.constructor_values(),
            "schedule": (nonfinal,),
            "lineup": (generation.lineup[0],),
            "matchsheet": (),
            "dispositions": (
                RequestDisposition(
                    endpoint="lineup",
                    state=DispositionState.CAPTURED,
                    detail="one side only",
                    event_id=nonfinal.event_id,
                ),
            ),
        }
    )
    assert "lineup two-side completeness" in " ".join(
        validate_scope_generation(one_side).failures
    )


@pytest.mark.unit
def test_schedule_flag_invariants_fail_closed():
    generation = _generation()
    impossible = ScheduleRow(
        **{
            **asdict(generation.schedule[0]),
            "terminal": False,
            "played_final": True,
            "terminal_nonplayed": True,
            "summary_required": False,
        }
    )
    report = validate_scope_generation(
        ScopeGeneration(
            **{**generation.constructor_values(), "schedule": (impossible,)}
        )
    )
    assert "schedule status flag invariant" in " ".join(report.failures)


@pytest.mark.unit
def test_durable_ledger_is_written_verified_and_reconstructable_before_manifest():
    generation = _generation()
    writer = FakeWriter()
    repository = EspnBronzeRepository(
        writer=writer,
        query=PhysicalQuery(generation),
        verify_physical=True,
        ensure_objects_on_write=False,
    )
    repository.publish_scope(generation)
    tables = [table for table, _ in writer.calls]
    assert tables == [
        *ENTITY_TABLES.values(),
        repository_module.LEDGER_TABLE,
        MANIFEST_TABLE,
    ]
    ledger_frame = writer.calls[-2][1]
    rebuilt = tuple(
        RawLedgerRecord.from_physical_row(row)
        for row in ledger_frame.to_dict(orient="records")
    )
    assert rebuilt == generation.raw_ledger
    manifest = writer.calls[-1][1].iloc[0]
    assert manifest["ledger_count"] == len(generation.raw_ledger)
    assert len(manifest["ledger_hash"]) == 64
    assert len(manifest["planned_request_ids_sha256"]) == 64


@pytest.mark.unit
@pytest.mark.parametrize(
    "mutation",
    [
        ("schedule", "home_score", True),
        ("schedule", "venue", 123),
        ("lineup", "home_away", "HOME"),
        ("lineup", "jersey", 9),
        ("lineup", "statistics_json", '{"z":1, "a":2}'),
        ("matchsheet", "attendance", True),
        ("matchsheet", "referee", 123),
        ("matchsheet", "extra_json", "not-json"),
    ],
)
def test_exact_row_type_matrix_rejects_coercions(mutation):
    generation = _generation()
    entity, field_name, value = mutation
    row_type = type(getattr(generation, entity)[0])
    bad_row = row_type(**{**asdict(getattr(generation, entity)[0]), field_name: value})
    with pytest.raises((TypeError, ValueError)):
        ScopeGeneration(**{**generation.constructor_values(), entity: (bad_row,)})


@pytest.mark.unit
def test_exact_row_type_matrix_rejects_string_backed_enum():
    generation = _generation()
    bad = ScheduleRow(
        **{
            **asdict(generation.schedule[0]),
            "status": DispositionState.CAPTURED,
        }
    )
    with pytest.raises(TypeError, match="schedule.status"):
        ScopeGeneration(**{**generation.constructor_values(), "schedule": (bad,)})


@pytest.mark.unit
def test_dq_requires_legacy_timestamp_and_captured_summary_dispositions():
    generation = _generation()
    drifted = ScheduleRow(
        **{
            **asdict(generation.schedule[0]),
            "date": datetime(2035, 1, 1, tzinfo=UTC),
            "match_date": datetime(2036, 1, 1, tzinfo=UTC),
        }
    )
    drifted_report = validate_scope_generation(
        ScopeGeneration(**{**generation.constructor_values(), "schedule": (drifted,)})
    )
    assert "schedule legacy timestamps" in " ".join(drifted_report.failures)

    nonfinal = ScheduleRow(
        **{
            **asdict(generation.schedule[0]),
            "terminal": False,
            "played_final": False,
            "summary_required": False,
            "home_score": None,
            "away_score": None,
            "home_goals": None,
            "away_goals": None,
        }
    )
    missing_dispositions = ScopeGeneration(
        **{
            **generation.constructor_values(),
            "schedule": (nonfinal,),
            "lineup": (),
            "matchsheet": (),
            "dispositions": (),
        }
    )
    assert "captured Summary disposition missing" in " ".join(
        validate_scope_generation(missing_dispositions).failures
    )


@pytest.mark.unit
def test_raw_ledger_reconstruction_normalizes_naive_trino_timestamp_to_utc():
    record = _generation().raw_ledger[-1]
    physical = {
        **record.constructor_values(),
        "disposition": record.disposition.value,
        "event_ids_json": canonical_json(record.event_ids),
        "fetched_at": record.fetched_at.replace(tzinfo=None),
    }
    rebuilt = RawLedgerRecord.from_physical_row(physical)
    assert rebuilt == record
    assert rebuilt.fetched_at.tzinfo is UTC


@pytest.mark.unit
def test_table_partition_map_matches_real_writer_contract():
    assert repository_module.TABLE_PARTITIONS == {
        **{table: ("scope_id",) for table in ENTITY_TABLES.values()},
        repository_module.LEDGER_TABLE: ("scope_id",),
        MANIFEST_TABLE: ("scope_id",),
        CUTOVER_TABLE: ("scope_id",),
        CATALOG_TABLE: ("snapshot_id",),
    }
    writer = FakeWriter()
    repository = EspnBronzeRepository(
        writer=writer, query=FakeQuery(), ensure_objects_on_write=False
    )
    snapshot = build_catalog_snapshot(
        snapshot_id="registry-20260731-v2",
        registry_signature=SIG_A,
        captured_at=datetime(2026, 7, 31, 7, tzinfo=UTC),
        run_id="discovery-2",
        raw_uri="s3://raw/catalog/v2.json.gz",
        raw_sha256=SIG_B,
        parser_version="espn-native-parser-v2",
        runtime_version="espn-native-runtime-v2",
        ingested_at=datetime(2026, 7, 31, 8, tzinfo=UTC),
        batch_id="catalog-batch-2",
        competitions=({"espn_id": 700, "slug": "eng.1"},),
    )
    assert isinstance(snapshot, repository_module.CatalogSnapshot)
    repository.append_catalog_snapshot(snapshot)
    assert writer.calls[0][0] == CATALOG_TABLE
    assert writer.options[0]["partition_spec"] == [("snapshot_id", "identity")]


@pytest.mark.unit
def test_catalog_snapshot_retry_is_idempotent_and_conflicts_fail_closed():
    captured_at = datetime(2026, 7, 31, 7, tzinfo=UTC)
    snapshot = build_catalog_snapshot(
        snapshot_id="registry-cas-1",
        registry_signature=SIG_A,
        captured_at=captured_at,
        run_id="discovery-cas-1",
        raw_uri="s3://raw/catalog/cas-1.json.gz",
        raw_sha256=SIG_B,
        parser_version="espn-native-parser-v2",
        runtime_version="espn-native-runtime-v2",
        ingested_at=captured_at,
        batch_id="catalog-cas-1",
        competitions=(
            {"espn_id": 700, "slug": "eng.1"},
            {"espn_id": 730, "slug": "ita.1"},
        ),
    )
    writer = FakeWriter()
    query = RepositoryStateQuery()
    repository = EspnBronzeRepository(
        writer=writer,
        query=query,
        ensure_objects_on_write=False,
    )

    repository.append_catalog_snapshot(snapshot)
    assert len(writer.calls) == 1
    query.catalog_rows = [
        (
            int(row["competition_id"]),
            str(row["record_sha256"]),
            snapshot.snapshot_signature,
        )
        for row in snapshot.rows
    ]
    repository.append_catalog_snapshot(snapshot)
    assert len(writer.calls) == 1

    conflict = build_catalog_snapshot(
        snapshot_id=snapshot.snapshot_id,
        registry_signature=SIG_A,
        captured_at=captured_at,
        run_id="discovery-cas-1",
        raw_uri="s3://raw/catalog/cas-1.json.gz",
        raw_sha256=SIG_B,
        parser_version="espn-native-parser-v2",
        runtime_version="espn-native-runtime-v2",
        ingested_at=captured_at,
        batch_id="catalog-cas-1",
        competitions=(
            {"espn_id": 700, "slug": "eng.1"},
            {"espn_id": 730, "slug": "ita.changed"},
        ),
    )
    with pytest.raises(ManifestConflictError, match="snapshot_id"):
        repository.append_catalog_snapshot(conflict)
    assert len(writer.calls) == 1


def _native_cutover(generation: ScopeGeneration, **changes):
    manifest = generation.manifest_row()
    values = {
        "cutover_id": "cutover-cas-1",
        "scope_id": generation.plan.scope_id,
        "active_source": "native",
        "previous_source": "legacy",
        "predecessor_cutover_id": None,
        "predecessor_cutover_sha256": None,
        "legacy_league": "ITA-Serie A",
        "legacy_season": "2021",
        "registry_signature": generation.registry_signature,
        "effective_at": datetime(2026, 7, 31, 10, tzinfo=UTC),
        "native_generation_id": generation.generation_id,
        "native_generation_signature": generation.generation_signature,
        "native_manifest_sha256": manifest["manifest_sha256"],
        "rollback_run_id": None,
        "rollback_reason": None,
        "metadata": {"approved_by": "test"},
    }
    values.update(changes)
    return repository_module.ScopeCutover(**values)


def _rollback_cutover(parent, *, cutover_id="rollback-graph-a"):
    return repository_module.ScopeCutover(
        **{
            **parent.constructor_values(),
            "cutover_id": cutover_id,
            "active_source": "legacy",
            "previous_source": "native",
            "predecessor_cutover_id": parent.cutover_id,
            "predecessor_cutover_sha256": parent.cutover_sha256,
            "effective_at": datetime(2026, 7, 31, 11, tzinfo=UTC),
            "native_generation_id": None,
            "native_generation_signature": None,
            "native_manifest_sha256": None,
            "rollback_run_id": f"{cutover_id}-run",
            "rollback_reason": "graph validation",
            "ancestor_cutover_sha256s": (
                *parent.ancestor_cutover_sha256s,
                parent.cutover_sha256,
            ),
        }
    )


def _cutover_graph_chain(generation):
    parent = _native_cutover(generation)
    branch = _rollback_cutover(parent)
    child = _native_cutover(
        generation,
        cutover_id="native-graph-c",
        predecessor_cutover_id=branch.cutover_id,
        predecessor_cutover_sha256=branch.cutover_sha256,
        effective_at=datetime(2026, 7, 31, 12, tzinfo=UTC),
        ancestor_cutover_sha256s=(
            parent.cutover_sha256,
            branch.cutover_sha256,
        ),
    )
    return parent, branch, child


def _cutover_graph_row(cutover, **changes):
    row = cutover.to_row()
    row.update(changes)
    return tuple(row[column] for column in CUTOVER_GRAPH_COLUMNS)


def _assert_cutover_graph_rejected_before_views(rows, match):
    query = CutoverGraphQuery(rows)
    repository = EspnBronzeRepository(writer=FakeWriter(), query=query)

    with pytest.raises(PublicationError, match=match):
        repository.ensure_objects()
    assert not any(sql.startswith("CREATE OR REPLACE VIEW") for sql, _ in query.calls)


@pytest.mark.unit
def test_ensure_objects_accepts_complete_cutover_graph_and_exact_duplicates():
    parent, branch, child = _cutover_graph_chain(_generation())
    sibling = _rollback_cutover(parent, cutover_id="rollback-graph-b")
    branch_row = _cutover_graph_row(branch)
    query = CutoverGraphQuery(
        (
            _cutover_graph_row(parent),
            branch_row,
            branch_row,
            _cutover_graph_row(sibling),
            _cutover_graph_row(child),
        )
    )
    repository = EspnBronzeRepository(writer=FakeWriter(), query=query)

    repository.ensure_objects()

    assert sum(
        sql.startswith("CREATE OR REPLACE VIEW") for sql, _ in query.calls
    ) == len(ENTITY_TABLES)


@pytest.mark.unit
def test_ensure_objects_rejects_truncated_cutover_ancestry_before_views():
    parent, branch, child = _cutover_graph_chain(_generation())
    truncated = (branch.cutover_sha256,)
    _assert_cutover_graph_rejected_before_views(
        (
            _cutover_graph_row(parent),
            _cutover_graph_row(branch),
            _cutover_graph_row(
                child,
                ancestor_cutover_sha256_json=canonical_json(truncated),
                ancestor_lineage_sha256=repository_module.canonical_sha256(truncated),
            ),
        ),
        "extend",
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "malformation",
    (
        "invalid_json",
        "noncanonical_json",
        "uppercase_sha",
        "uppercase_cutover_sha",
        "duplicate_ancestor",
        "incomplete_predecessor",
    ),
)
def test_ensure_objects_rejects_nonnull_malformed_cutover_ancestry_before_views(
    malformation,
):
    parent, branch, _ = _cutover_graph_chain(_generation())
    changes = {}
    if malformation == "invalid_json":
        changes["ancestor_cutover_sha256_json"] = "[not-json]"
    elif malformation == "noncanonical_json":
        changes["ancestor_cutover_sha256_json"] = f'[ "{parent.cutover_sha256}" ]'
    elif malformation == "uppercase_sha":
        ancestors = (parent.cutover_sha256.upper(),)
        changes.update(
            ancestor_cutover_sha256_json=canonical_json(ancestors),
            ancestor_lineage_sha256=repository_module.canonical_sha256(ancestors),
        )
    elif malformation == "uppercase_cutover_sha":
        changes["cutover_sha256"] = branch.cutover_sha256.upper()
    elif malformation == "duplicate_ancestor":
        ancestors = (parent.cutover_sha256, parent.cutover_sha256)
        changes.update(
            ancestor_cutover_sha256_json=canonical_json(ancestors),
            ancestor_lineage_sha256=repository_module.canonical_sha256(ancestors),
        )
    else:
        changes["predecessor_cutover_id"] = None
    _assert_cutover_graph_rejected_before_views(
        (_cutover_graph_row(parent), _cutover_graph_row(branch, **changes)),
        "ancestry migration",
    )


@pytest.mark.unit
def test_ensure_objects_rejects_cutover_ancestry_hash_mismatch_before_views():
    parent, branch, _ = _cutover_graph_chain(_generation())
    _assert_cutover_graph_rejected_before_views(
        (
            _cutover_graph_row(parent),
            _cutover_graph_row(branch, ancestor_lineage_sha256="0" * 64),
        ),
        "hash",
    )


@pytest.mark.unit
def test_ensure_objects_rejects_missing_and_cross_scope_cutover_predecessors():
    parent, branch, child = _cutover_graph_chain(_generation())
    _assert_cutover_graph_rejected_before_views(
        (_cutover_graph_row(parent), _cutover_graph_row(child)),
        "predecessor",
    )
    _assert_cutover_graph_rejected_before_views(
        (
            _cutover_graph_row(parent),
            _cutover_graph_row(branch, scope_id="740:2020"),
        ),
        "scope",
    )


@pytest.mark.unit
def test_ensure_objects_rejects_conflicting_and_cyclic_cutover_nodes():
    parent, branch, _ = _cutover_graph_chain(_generation())
    conflicting_hash = "e" * 64
    _assert_cutover_graph_rejected_before_views(
        (
            _cutover_graph_row(parent),
            _cutover_graph_row(branch),
            _cutover_graph_row(branch, cutover_sha256=conflicting_hash),
        ),
        "conflicting",
    )
    ambiguous_ancestors = ("d" * 64, parent.cutover_sha256)
    _assert_cutover_graph_rejected_before_views(
        (
            _cutover_graph_row(parent),
            _cutover_graph_row(branch),
            _cutover_graph_row(
                branch,
                ancestor_cutover_sha256_json=canonical_json(ambiguous_ancestors),
                ancestor_lineage_sha256=repository_module.canonical_sha256(
                    ambiguous_ancestors
                ),
            ),
        ),
        "conflicting",
    )
    _assert_cutover_graph_rejected_before_views(
        (
            _cutover_graph_row(parent),
            _cutover_graph_row(branch),
            _cutover_graph_row(branch, cutover_id="hash-alias"),
        ),
        "hash bound to conflicting",
    )

    cyclic_hash = "9" * 64
    cyclic = (
        "cyclic-cutover",
        parent.scope_id,
        cyclic_hash,
        "cyclic-cutover",
        cyclic_hash,
        canonical_json((cyclic_hash,)),
        repository_module.canonical_sha256((cyclic_hash,)),
    )
    _assert_cutover_graph_rejected_before_views((cyclic,), "extend|cyclic")


@pytest.mark.unit
def test_ensure_objects_rejects_invalid_stored_root_and_child_shapes():
    parent, branch, _ = _cutover_graph_chain(_generation())
    root_ancestors = ("f" * 64,)
    _assert_cutover_graph_rejected_before_views(
        (
            _cutover_graph_row(
                parent,
                ancestor_cutover_sha256_json=canonical_json(root_ancestors),
                ancestor_lineage_sha256=repository_module.canonical_sha256(
                    root_ancestors
                ),
            ),
        ),
        "root",
    )
    _assert_cutover_graph_rejected_before_views(
        (
            _cutover_graph_row(parent),
            _cutover_graph_row(
                branch,
                ancestor_cutover_sha256_json=canonical_json(()),
                ancestor_lineage_sha256=repository_module.canonical_sha256(()),
            ),
        ),
        "child",
    )


@pytest.mark.unit
def test_cutover_contract_persists_canonical_immutable_ancestor_lineage():
    generation = _generation()
    parent = _native_cutover(generation)
    child = repository_module.ScopeCutover(
        cutover_id="rollback-lineage-1",
        scope_id=generation.plan.scope_id,
        active_source="legacy",
        previous_source="native",
        predecessor_cutover_id=parent.cutover_id,
        predecessor_cutover_sha256=parent.cutover_sha256,
        legacy_league=parent.legacy_league,
        legacy_season=parent.legacy_season,
        registry_signature=parent.registry_signature,
        effective_at=datetime(2026, 7, 31, 11, tzinfo=UTC),
        native_generation_id=None,
        native_generation_signature=None,
        native_manifest_sha256=None,
        rollback_run_id="rollback-lineage-run-1",
        rollback_reason="lineage test",
        metadata={"approved_by": "test"},
        ancestor_cutover_sha256s=(parent.cutover_sha256,),
    )
    row = child.to_row()
    assert row["ancestor_cutover_sha256_json"] == canonical_json(
        (parent.cutover_sha256,)
    )
    assert len(row["ancestor_lineage_sha256"]) == 64
    with pytest.raises(ValueError, match="ancestry"):
        repository_module.ScopeCutover(
            **{
                **child.constructor_values(),
                "ancestor_cutover_sha256s": (),
            }
        )


@pytest.mark.unit
def test_cutover_repository_rejects_descendant_while_scope_fork_is_unresolved():
    generation = _generation()
    parent = _native_cutover(generation)
    branch = repository_module.ScopeCutover(
        cutover_id="rollback-branch-a",
        scope_id=generation.plan.scope_id,
        active_source="legacy",
        previous_source="native",
        predecessor_cutover_id=parent.cutover_id,
        predecessor_cutover_sha256=parent.cutover_sha256,
        legacy_league=parent.legacy_league,
        legacy_season=parent.legacy_season,
        registry_signature=parent.registry_signature,
        effective_at=datetime(2026, 7, 31, 11, tzinfo=UTC),
        native_generation_id=None,
        native_generation_signature=None,
        native_manifest_sha256=None,
        rollback_run_id="rollback-branch-a-run",
        rollback_reason="branch A",
        metadata={"approved_by": "test"},
        ancestor_cutover_sha256s=(parent.cutover_sha256,),
    )
    descendant = _native_cutover(
        generation,
        cutover_id="native-descendant-c",
        predecessor_cutover_id=branch.cutover_id,
        predecessor_cutover_sha256=branch.cutover_sha256,
        effective_at=datetime(2026, 7, 31, 12, tzinfo=UTC),
        ancestor_cutover_sha256s=(
            parent.cutover_sha256,
            branch.cutover_sha256,
        ),
    )
    query = RepositoryStateQuery()
    query.unresolved_cutover_forks.add(generation.plan.scope_id)
    query.latest_cutovers[generation.plan.scope_id] = (
        branch.cutover_id,
        branch.cutover_sha256,
        "legacy",
        branch.effective_at,
        canonical_json(branch.ancestor_cutover_sha256s),
        branch.ancestor_lineage_sha256,
    )
    manifest = generation.manifest_row()
    query.manifests[(generation.plan.scope_id, generation.generation_id)] = tuple(
        manifest[column] for column in EspnBronzeRepository.manifest_columns
    )
    writer = FakeWriter()
    repository = EspnBronzeRepository(
        writer=writer,
        query=query,
        ensure_objects_on_write=False,
    )
    with pytest.raises(ManifestConflictError, match="unresolved cutover fork"):
        repository.append_cutover(descendant)
    assert not writer.calls


@pytest.mark.unit
def test_cutover_repository_exact_retry_remains_noop_after_scope_fork():
    generation = _generation()
    cutover = _native_cutover(generation)
    query = RepositoryStateQuery()
    query.cutover_hashes[cutover.cutover_id] = [cutover.cutover_sha256]
    query.unresolved_cutover_forks.add(generation.plan.scope_id)
    writer = FakeWriter()
    repository = EspnBronzeRepository(
        writer=writer,
        query=query,
        ensure_objects_on_write=False,
    )

    assert repository.append_cutover(cutover) == f"iceberg.bronze.{CUTOVER_TABLE}"
    assert not writer.calls
    assert not any("unresolved_cutover_forks" in sql for sql, _ in query.calls)


@pytest.mark.unit
def test_cutover_repository_fork_probe_detects_global_conflicting_ids():
    generation = _generation()
    cutover = _native_cutover(generation)
    query = RepositoryStateQuery()
    # Models the same cutover_id having another hash in another scope.
    query.unresolved_cutover_forks.add(generation.plan.scope_id)
    repository = EspnBronzeRepository(
        writer=FakeWriter(),
        query=query,
        ensure_objects_on_write=False,
    )
    with pytest.raises(ManifestConflictError, match="unresolved cutover fork"):
        repository.append_cutover(cutover)
    probe_sql = next(sql for sql, _ in query.calls if "unresolved_cutover_forks" in sql)
    conflicting_ids = probe_sql[
        probe_sql.index("conflicting_ids AS") : probe_sql.index(
            "), conflicting_predecessors AS"
        )
    ]
    assert f"FROM iceberg.bronze.{CUTOVER_TABLE}" in conflicting_ids
    assert "FROM scope_cutovers" not in conflicting_ids


@pytest.mark.unit
def test_current_view_excludes_forks_conflicting_ids_and_all_descendants():
    sql = render_current_view_sql("lineup")
    ddl = render_repository_ddl()[CUTOVER_TABLE]
    assert '"ancestor_cutover_sha256_json"' in ddl
    assert '"ancestor_lineage_sha256"' in ddl
    assert "bad_cutover_hashes AS" in sql
    assert "invalid_lineage_hashes AS" in sql
    assert "eligible_cutovers AS" in sql
    assert "ancestor_cutover_sha256s" in sql
    assert "CONTAINS(c.ancestor_cutover_sha256s, bad.cutover_sha256)" in sql
    assert "lineage_valid_cutovers AS" in sql
    assert "FROM lineage_valid_cutovers c" in sql
    assert "FROM eligible_cutovers c" in sql
    assert "parent.scope_id = child.scope_id" in sql
    assert "parent.cutover_id = child.predecessor_cutover_id" in sql
    assert "parent.cutover_sha256 = child.predecessor_cutover_sha256" in sql
    assert (
        "CONCAT(parent.ancestor_cutover_sha256s, ARRAY[parent.cutover_sha256])" in sql
    )
    assert "json_format(CAST(parsed.ancestor_cutover_sha256s AS JSON))" in sql
    assert "cardinality(array_distinct(parsed.ancestor_cutover_sha256s))" in sql

    p, a, b, c, q, x1, x2, d = (digit * 64 for digit in "12345678")
    malformed_d = "9" * 64
    propagated_e = "a" * 64
    records = (
        ("scope-1", "P", p, None, None, ()),
        ("scope-1", "P", p, None, None, ()),  # exact duplicate, not a fork
        ("scope-1", "A", a, "P", p, (p,)),
        ("scope-1", "B", b, "P", p, (p,)),  # direct sibling fork
        ("scope-1", "C", c, "A", a, (p, a)),  # descendant of fork A
        ("scope-1", "D", malformed_d, "C", c, (c,)),  # truncated lineage
        ("scope-1", "E", propagated_e, "D", malformed_d, (c, malformed_d)),
        ("scope-2", "Q", q, None, None, ()),
        ("scope-2", "X", x1, "Q", q, (q,)),
        ("scope-2", "X", x2, "Q", q, (q,)),  # conflicting immutable ID
        ("scope-2", "Z", d, "X", x1, (q, x1)),  # conflict descendant
    )
    id_hashes: dict[str, set[str]] = {}
    slot_hashes: dict[tuple[str, str | None], set[str]] = {}
    for scope_id, cutover_id, cutover_hash, _, predecessor, _ in records:
        id_hashes.setdefault(cutover_id, set()).add(cutover_hash)
        slot_hashes.setdefault((scope_id, predecessor), set()).add(cutover_hash)
    bad_hashes = {
        cutover_hash
        for scope_id, cutover_id, cutover_hash, _, predecessor, _ in records
        if len(id_hashes[cutover_id]) > 1
        or len(slot_hashes[(scope_id, predecessor)]) > 1
    }
    lineage_valid = {
        cutover_hash
        for scope_id, _, cutover_hash, predecessor_id, predecessor_hash, ancestors in records
        if (predecessor_id is None and predecessor_hash is None and not ancestors)
        or len(
            {
                parent
                for parent in records
                if parent[0] == scope_id
                and parent[1] == predecessor_id
                and parent[2] == predecessor_hash
                and ancestors == (*parent[5], parent[2])
            }
        )
        == 1
    }
    eligible_without_invalid_propagation = {
        cutover_hash
        for _, _, cutover_hash, _, _, ancestors in records
        if cutover_hash in lineage_valid
        and cutover_hash not in bad_hashes
        and not bad_hashes.intersection(ancestors)
    }
    assert propagated_e in eligible_without_invalid_propagation
    invalid_lineage_hashes = {
        cutover_hash
        for _, _, cutover_hash, _, _, _ in records
        if cutover_hash not in lineage_valid
    }
    bad_hashes.update(invalid_lineage_hashes)
    eligible = {
        cutover_hash
        for _, _, cutover_hash, _, _, ancestors in records
        if cutover_hash in lineage_valid
        and cutover_hash not in bad_hashes
        and not bad_hashes.intersection(ancestors)
    }
    assert malformed_d not in lineage_valid
    assert malformed_d in invalid_lineage_hashes
    assert propagated_e not in eligible
    assert eligible == {p, q}


@pytest.mark.unit
def test_cutover_repository_enforces_readiness_idempotency_and_conflict_cas():
    generation = _generation()
    cutover = _native_cutover(generation)
    writer = FakeWriter()
    query = RepositoryStateQuery()
    repository = EspnBronzeRepository(
        writer=writer,
        query=query,
        verify_physical=False,
        ensure_objects_on_write=False,
    )

    with pytest.raises(PublicationError, match="COMPLETE manifest"):
        repository.append_cutover(cutover)
    assert not writer.calls

    manifest = generation.manifest_row()
    query.manifests[(generation.plan.scope_id, generation.generation_id)] = tuple(
        manifest[column] for column in repository.manifest_columns
    )
    repository.append_cutover(cutover)
    assert [table for table, _ in writer.calls] == [CUTOVER_TABLE]
    assert writer.options[-1]["partition_spec"] == [("scope_id", "identity")]

    query.cutover_hashes[cutover.cutover_id] = [cutover.cutover_sha256]
    query.cutover_slots[(cutover.scope_id, None)] = [
        (cutover.cutover_id, cutover.cutover_sha256)
    ]
    repository.append_cutover(cutover)
    assert len(writer.calls) == 1

    conflicting_retry = _native_cutover(
        generation,
        metadata={"approved_by": "different-operator"},
    )
    with pytest.raises(ManifestConflictError, match="cutover_id"):
        repository.append_cutover(conflicting_retry)
    assert len(writer.calls) == 1


@pytest.mark.unit
def test_cutover_repository_enforces_transition_chain_and_timestamp():
    generation = _generation()
    native = _native_cutover(generation)
    query = RepositoryStateQuery()
    query.manifests[(generation.plan.scope_id, generation.generation_id)] = tuple(
        generation.manifest_row()[column]
        for column in EspnBronzeRepository.manifest_columns
    )
    query.latest_cutovers[generation.plan.scope_id] = (
        native.cutover_id,
        native.cutover_sha256,
        "native",
        native.effective_at,
        canonical_json(native.ancestor_cutover_sha256s),
        native.ancestor_lineage_sha256,
    )
    writer = FakeWriter()
    repository = EspnBronzeRepository(
        writer=writer,
        query=query,
        ensure_objects_on_write=False,
    )
    rollback = repository_module.ScopeCutover(
        cutover_id="rollback-cas-1",
        scope_id=generation.plan.scope_id,
        active_source="legacy",
        previous_source="native",
        predecessor_cutover_id=native.cutover_id,
        predecessor_cutover_sha256=native.cutover_sha256,
        legacy_league=native.legacy_league,
        legacy_season=native.legacy_season,
        registry_signature=native.registry_signature,
        effective_at=datetime(2026, 7, 31, 11, tzinfo=UTC),
        native_generation_id=None,
        native_generation_signature=None,
        native_manifest_sha256=None,
        rollback_run_id="rollback-run-cas-1",
        rollback_reason="operator rollback",
        metadata={"approved_by": "test"},
        ancestor_cutover_sha256s=(native.cutover_sha256,),
    )
    repository.append_cutover(rollback)
    assert writer.calls[-1][1].iloc[0]["cutover_sha256"] == rollback.cutover_sha256

    stale = repository_module.ScopeCutover(
        **{
            **rollback.constructor_values(),
            "cutover_id": "rollback-cas-stale",
            "effective_at": native.effective_at,
        }
    )
    with pytest.raises(ManifestConflictError, match="effective_at"):
        repository.append_cutover(stale)

    query.latest_cutovers[generation.plan.scope_id] = (
        rollback.cutover_id,
        rollback.cutover_sha256,
        "legacy",
        rollback.effective_at,
        canonical_json(rollback.ancestor_cutover_sha256s),
        rollback.ancestor_lineage_sha256,
    )
    with pytest.raises(ManifestConflictError, match="previous_source"):
        repository.append_cutover(
            repository_module.ScopeCutover(
                **{
                    **rollback.constructor_values(),
                    "cutover_id": "rollback-cas-invalid-chain",
                    "predecessor_cutover_id": rollback.cutover_id,
                    "predecessor_cutover_sha256": rollback.cutover_sha256,
                    "ancestor_cutover_sha256s": (
                        native.cutover_sha256,
                        rollback.cutover_sha256,
                    ),
                    "effective_at": datetime(2026, 7, 31, 12, tzinfo=UTC),
                }
            )
        )

    query.cutover_slots[(generation.plan.scope_id, native.cutover_sha256)] = [
        (rollback.cutover_id, rollback.cutover_sha256)
    ]
    with pytest.raises(ManifestConflictError, match="different successor"):
        repository.append_cutover(
            repository_module.ScopeCutover(
                **{
                    **rollback.constructor_values(),
                    "cutover_id": "rollback-cas-fork",
                    "effective_at": datetime(2026, 7, 31, 13, tzinfo=UTC),
                }
            )
        )


@pytest.mark.unit
def test_cutover_contract_cas_readiness_and_deterministic_rollback():
    cutover_type = repository_module.ScopeCutover
    native = cutover_type(
        cutover_id="cutover-1",
        scope_id="730:2020",
        active_source="native",
        previous_source="legacy",
        predecessor_cutover_id=None,
        predecessor_cutover_sha256=None,
        legacy_league="ITA-Serie A",
        legacy_season="2021",
        registry_signature=SIG_A,
        effective_at=datetime(2026, 7, 31, 10, tzinfo=UTC),
        native_generation_id="generation-1",
        native_generation_signature="c" * 64,
        native_manifest_sha256="d" * 64,
        rollback_run_id=None,
        rollback_reason=None,
        metadata={"approved_by": "test"},
    )
    assert len(native.cutover_sha256) == 64
    with pytest.raises(ValueError):
        cutover_type(**{**native.constructor_values(), "previous_source": "native"})
    rollback = cutover_type(
        **{
            **native.constructor_values(),
            "cutover_id": "rollback-1",
            "active_source": "legacy",
            "previous_source": "native",
            "predecessor_cutover_id": native.cutover_id,
            "predecessor_cutover_sha256": native.cutover_sha256,
            "effective_at": datetime(2026, 7, 31, 11, tzinfo=UTC),
            "native_generation_id": None,
            "native_generation_signature": None,
            "native_manifest_sha256": None,
            "rollback_run_id": "rollback-run-1",
            "rollback_reason": "operator rollback",
            "ancestor_cutover_sha256s": (native.cutover_sha256,),
        }
    )
    assert rollback.cutover_sha256 != native.cutover_sha256
    sql = render_current_view_sql("schedule")
    assert "effective_at DESC, cutover_id DESC, cutover_sha256 DESC" in sql
    assert "conflicting_cutover_predecessors" in sql
