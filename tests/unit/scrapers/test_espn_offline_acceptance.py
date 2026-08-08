"""Task 7 offline acceptance: one immutable ESPN Native evidence chain."""

from __future__ import annotations

from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import socket

import duckdb
import pytest
import sqlglot
from sqlglot import exp

from scrapers.espn.canary_campaign import CampaignIdentity
from scrapers.espn.discovery import discover_catalog
from scrapers.espn.models import AgeClass
from scrapers.espn.operations import (
    MemoryScopeLeaseStore,
    RunManifestEvidence,
    ScopeHead,
)
from scrapers.espn.raw_store import EspnRawStore
from scrapers.espn.registry import promote_candidate, validate_registry_document
from scrapers.espn.repository import (
    CUTOVER_TABLE,
    ENTITY_TABLES,
    MANIFEST_TABLE,
    EspnBronzeRepository,
    PublicationError,
    ScopeCutover,
    ScopeGeneration,
    render_current_view_sql,
    validate_scope_generation,
)
from scrapers.espn.runner import execute
from tests.unit.scrapers.test_run_espn_scraper import (
    FakeHttpClient,
    TEST_RELEASE_COMMIT,
    TEST_RELEASE_TREE_SHA256,
    _bind_current_plan_artifact_chain,
    _bind_replay_to_capture,
    _plan,
    _rewrite_signed_plan,
    _scoreboard,
)


@pytest.fixture(autouse=True)
def _current_release_env(monkeypatch):
    monkeypatch.setenv("ESPN_RELEASE_COMMIT", TEST_RELEASE_COMMIT)
    monkeypatch.setenv("ESPN_RELEASE_TREE_SHA256", TEST_RELEASE_TREE_SHA256)


class _DuckDbBronzeBackend:
    """Stateful in-memory adapter for the real production repository."""

    def __init__(self) -> None:
        self.connection = duckdb.connect(":memory:")
        self.connection.execute("CREATE SCHEMA bronze")

    def write_dataframe(self, frame, *, database, table, **_kwargs):
        normalized = frame.copy()
        # Pandas/DuckDB otherwise infer an all-null VARCHAR control column as
        # INTEGER. Production Iceberg gets its type from DDL instead.
        for column in normalized:
            if normalized[column].isna().all():
                normalized[column] = normalized[column].astype("string")
        self.connection.register("_espn_incoming", normalized)
        try:
            self.connection.execute(
                f'CREATE TABLE IF NOT EXISTS "{database}"."{table}" '
                "AS SELECT * FROM _espn_incoming WHERE FALSE"
            )
            self.connection.execute(
                f'INSERT INTO "{database}"."{table}" BY NAME '
                "SELECT * FROM _espn_incoming"
            )
        finally:
            self.connection.unregister("_espn_incoming")
        return f"memory.{database}.{table}"

    def execute_query(self, sql, params=None):
        try:
            return self.connection.execute(sql, params or ()).fetchall()
        except duckdb.CatalogException as exc:
            # Append-only tables do not exist until their first production
            # repository write in this small in-memory backend.
            if "does not exist" in str(exc):
                return []
            raise


class _RecordingProductionRepository(EspnBronzeRepository):
    def __init__(self, backend: _DuckDbBronzeBackend) -> None:
        super().__init__(
            writer=backend,
            query=backend,
            catalog="memory",
            schema="bronze",
            verify_physical=False,
            ensure_objects_on_write=False,
        )
        self.generations: list[ScopeGeneration] = []

    def publish_scope(self, generation, **kwargs):
        result = super().publish_scope(generation, **kwargs)
        self.generations.append(generation)
        return result


def _duckdb_current_view_sql(entity: str) -> str:
    """Translate the full production Trino view, preserving its predicates."""

    tree = sqlglot.parse_one(
        render_current_view_sql(entity, catalog="memory", schema="bronze"),
        read="trino",
    )

    def translate(expression):
        # Trino SHA-256 returns bytes and to_hex renders them. DuckDB SHA-256
        # already returns the lower-case hexadecimal digest.
        if (
            isinstance(expression, exp.LowerHex)
            and isinstance(expression.this, exp.SHA2)
            and isinstance(expression.this.this, exp.Encode)
        ):
            return exp.Lower(
                this=exp.Anonymous(
                    this="SHA256",
                    expressions=[expression.this.this.this.copy()],
                )
            )
        if (
            isinstance(expression, exp.Anonymous)
            and expression.name.upper() == "ALL_MATCH"
        ):
            values, predicate = expression.expressions
            left = exp.Identifier(this="left_value")
            right = exp.Identifier(this="right_value")
            reducer = exp.Lambda(
                this=exp.And(this=left.copy(), expression=right.copy()),
                expressions=[left, right],
            )
            mapped = exp.Anonymous(
                this="LIST_TRANSFORM",
                expressions=[values.copy(), predicate.copy()],
            )
            return exp.Anonymous(
                this="LIST_REDUCE",
                expressions=[mapped, reducer, exp.Boolean(this=True)],
            )
        return expression

    return tree.transform(translate).sql(dialect="duckdb")


def _activate_and_assert_current(
    repository: _RecordingProductionRepository,
    backend: _DuckDbBronzeBackend,
    generation: ScopeGeneration,
    *,
    cutover_id: str,
    manifest_count: int = 1,
) -> None:
    cutover = ScopeCutover(
        cutover_id=cutover_id,
        scope_id=generation.plan.scope_id,
        active_source="native",
        previous_source="legacy",
        predecessor_cutover_id=None,
        predecessor_cutover_sha256=None,
        legacy_league=generation.schedule[0].league,
        legacy_season=generation.schedule[0].season,
        registry_signature=generation.registry_signature,
        effective_at=generation.ingested_at + timedelta(seconds=1),
        native_generation_id=generation.generation_id,
        native_generation_signature=generation.generation_signature,
        native_manifest_sha256=generation.manifest_sha256,
        rollback_run_id=None,
        rollback_reason=None,
        metadata={"evidence": "offline production acceptance"},
    )
    repository.append_cutover(cutover)

    for entity, table in ENTITY_TABLES.items():
        backend.connection.execute(
            f"CREATE TABLE bronze.espn_{entity} AS "
            f"SELECT * FROM bronze.{table} WHERE FALSE"
        )
        backend.connection.execute(_duckdb_current_view_sql(entity))

        rows = backend.connection.execute(
            f'SELECT DISTINCT "generation_id", "generation_signature" '
            f"FROM bronze.espn_{entity}_current"
        ).fetchall()
        assert rows == [(generation.generation_id, generation.generation_signature)]
        assert backend.connection.execute(
            f"SELECT COUNT(*) FROM bronze.espn_{entity}_current"
        ).fetchone() == (len(getattr(generation, entity)),)

    manifests = backend.connection.execute(
        f'SELECT "manifest_sha256" FROM bronze.{MANIFEST_TABLE}'
    ).fetchall()
    assert (generation.manifest_sha256,) in manifests
    assert len(manifests) == manifest_count
    assert backend.connection.execute(
        f'SELECT "cutover_sha256" FROM bronze.{CUTOVER_TABLE}'
    ).fetchall() == [(cutover.cutover_sha256,)]


def _record_control_head(
    store: MemoryScopeLeaseStore,
    generation: ScopeGeneration,
    *,
    published_at,
) -> ScopeHead:
    head = ScopeHead(
        dag_id="dag_ingest_espn",
        scope_id=generation.plan.scope_id,
        generation_id=generation.generation_id,
        generation_signature=generation.generation_signature,
        manifest_sha256=generation.manifest_sha256,
        snapshot_uri=f"file:///snapshots/{generation.generation_id}.json",
        snapshot_sha256=hashlib.sha256(generation.generation_id.encode()).hexdigest(),
        registry_signature=generation.registry_signature,
        plan_signature=generation.plan_signature,
        run_id=generation.run_id,
        published_at=published_at,
        completed_at=generation.ingested_at,
    )
    lease = store.acquire_many(
        (head.scope_id,),
        owner_id=f"dag_ingest_espn/{generation.run_id}/1",
        plan_signature=head.plan_signature,
        now=published_at - timedelta(seconds=1),
        ttl=timedelta(hours=1),
    )[0]
    evidence = RunManifestEvidence(
        dag_id=head.dag_id,
        run_id=head.run_id,
        attempt=1,
        scope_id=head.scope_id,
        plan_signature=head.plan_signature,
        registry_signature=head.registry_signature,
        state="complete",
        evidence_uri=f"file:///evidence/{generation.generation_id}.json",
        evidence_sha256=hashlib.sha256(generation.run_id.encode()).hexdigest(),
        recorded_at=published_at,
    )
    with store.publication_guard(lease, now=published_at) as fence:
        selected = fence.record_published(head, evidence)
    store.release(lease, now=published_at)
    return selected


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
    runtime = plan.metadata["runtime"]
    registry_uri = runtime["registry_snapshot_uri"]
    registry_body = registry.canonical_json().encode()
    Path(registry_uri.removeprefix("file://")).write_bytes(registry_body)
    release = {
        **CampaignIdentity.create(
            release_commit=runtime["release"]["release_commit"],
            release_tree_sha256=runtime["release"]["release_tree_sha256"],
            registry_signature=registry.signature(),
            target_scope_ids=runtime["release"]["target_scope_ids"],
        ).to_dict(),
        "parser_version": runtime["release"]["parser_version"],
        "runtime_version": runtime["release"]["runtime_version"],
    }
    admission_uri = runtime["admission_ref"]["uri"]
    admission_path = Path(admission_uri.removeprefix("file://"))
    admission = json.loads(admission_path.read_text())
    admission.update(
        {
            "registry_ref": {
                "uri": registry_uri,
                "sha256": hashlib.sha256(registry_body).hexdigest(),
            },
            "registry_signature": registry.signature(),
            "release": release,
        }
    )
    admission_body = json.dumps(
        admission, sort_keys=True, separators=(",", ":")
    ).encode()
    admission_path.write_bytes(admission_body)
    admission_ref = {
        "uri": admission_uri,
        "sha256": hashlib.sha256(admission_body).hexdigest(),
    }

    def mutate(document):
        document["registry_signature"] = registry.signature()
        document["metadata"]["runtime"]["release"] = release
        document["metadata"]["runtime"]["admission_ref"] = admission_ref

    rebuilt = _rewrite_signed_plan(
        options,
        plan,
        mutate,
    )
    _bind_current_plan_artifact_chain(options)
    return rebuilt


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
    capture_backend = _DuckDbBronzeBackend()
    capture_repository = _RecordingProductionRepository(capture_backend)
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
    delayed = ScopeGeneration(
        **{
            **generation.constructor_values(),
            "run_id": "offline-delayed-older",
            "generation_id": "offline-delayed-older-generation",
            "batch_id": "offline-delayed-older-batch",
            "ingested_at": generation.ingested_at - timedelta(days=1),
        }
    )
    capture_repository.publish_scope(delayed)
    equal_manual = ScopeGeneration(
        **{
            **generation.constructor_values(),
            "run_id": "offline-equal-manual",
            "generation_id": "zzzz-offline-equal-manual",
            "batch_id": "offline-equal-manual-batch",
        }
    )
    equal_retry = ScopeGeneration(
        **{
            **generation.constructor_values(),
            "run_id": "offline-equal-retry",
            "generation_id": "0000-offline-equal-retry",
            "batch_id": "offline-equal-retry-batch",
        }
    )
    capture_repository.publish_scope(equal_manual)
    capture_repository.publish_scope(equal_retry)
    control = MemoryScopeLeaseStore()
    selected = _record_control_head(
        control,
        generation,
        published_at=generation.ingested_at + timedelta(hours=1),
    )
    selected = _record_control_head(
        control,
        delayed,
        published_at=generation.ingested_at + timedelta(hours=2),
    )
    selected = _record_control_head(
        control,
        equal_manual,
        published_at=generation.ingested_at + timedelta(hours=3),
    )
    selected = _record_control_head(
        control,
        equal_retry,
        published_at=generation.ingested_at + timedelta(hours=4),
    )
    assert selected.generation_id == equal_manual.generation_id
    _activate_and_assert_current(
        capture_repository,
        capture_backend,
        equal_manual,
        cutover_id="offline-capture-cutover",
        manifest_count=4,
    )
    capture_repository.verify_current_scope_selection(equal_manual)
    for nonselected in (generation, delayed, equal_retry):
        with pytest.raises(PublicationError, match="current view selection"):
            capture_repository.verify_current_scope_selection(nonselected)
    for entity in ENTITY_TABLES:
        current_generation = capture_backend.connection.execute(
            f'SELECT DISTINCT "generation_id" FROM bronze.espn_{entity}_current'
        ).fetchone()[0]
        assert current_generation == selected.generation_id

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

    replay_backend = _DuckDbBronzeBackend()
    replay_repository = _RecordingProductionRepository(replay_backend)
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
    _activate_and_assert_current(
        replay_repository,
        replay_backend,
        replayed,
        cutover_id="offline-replay-cutover",
    )
