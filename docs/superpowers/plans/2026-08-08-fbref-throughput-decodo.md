# FBref Throughput and Decodo Efficiency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Sustain at least 1,500 fully persisted FBref matches per day while preserving every Bronze/control-plane datum and minimizing Decodo bytes per durable match.

**Architecture:** Keep the source-facing path conservative: one active lease, the existing 6.1-second domain interval, raw-first commits, and no network retry after raw bytes exist. Replace per-match Iceberg writes with bounded, table-oriented batches behind a default-off feature flag; completion remains per observation and is written only after generic, typed, availability, stateful, and control-plane effects succeed. Measure the sequential and batch paths on identical saved HTML in isolated schemas, then enable the batch path only when bidirectional content diffs and crash-recovery tests prove equivalence.

**Tech Stack:** Python 3, pandas/pyarrow, Trino, Apache Iceberg, PostgreSQL control plane, Airflow, pytest, Decodo residential proxies.

## Global Constraints

- Scope is every male FBref competition in the registry (approximately 117) for source seasons `2025-2026` and `2026-2027`.
- Correctness outranks throughput; throughput outranks proxy efficiency; DAG colour is diagnostic rather than the business outcome.
- Preserve all 12 match-page Bronze tables: eight typed data tables, `fbref_dataset_availability`, `fbref_table_cells`, `fbref_table_inventory`, and `fbref_page_manifest`.
- Preserve `clear`, `skip`, availability/reason, source IDs, content hashes, parser versions, manifests, latest/stale verdicts, and per-observation completion semantics.
- Save immutable raw bytes before parsing; every persistence retry after a raw commit must use zero proxy requests and zero proxy bytes.
- Do not claim cross-table atomic visibility. Guarantee eventual zero loss through idempotent replay, and never write `typed:__complete__` or mark `observation_processing` succeeded before every required effect completes.
- Keep the 6.1-second source interval, anti-ban classification, `publication_lock`, one active paid-proxy lease, and the driver guard that accepts a failed run only when its queue demonstrably moved.
- Do not create `state/history_enabled`, do not manipulate `publication_lock` manually, and do not commit proxy credentials.
- Decodo has no business-level traffic cap, but acceptance measures provider-billed bytes per durable match and warm-up share. A safety circuit breaker remains mandatory to stop runaway traffic.
- Reject the dashboard's default 10-minute sticky setting for production. Select a custom sticky duration from a canary that verifies exit-IP stability and Cloudflare reuse; rotate on transport failure rather than per request.
- Batch mode remains default-off as `FBREF_BATCH_PERSIST=0` until the differential and fault-injection gates pass.
- Batch limits are `FBREF_BATCH_PERSIST_MATCHES=8` (allowed range 2–25) and `FBREF_BATCH_PERSIST_MAX_CELLS=150000`.
- Replay acceptance requires at least 4× persistence speedup and no more than 20 seconds per match; production acceptance requires at least 1,500 durable matches/day.
- Production deployment, proxy-file replacement, container recreation, and data-remediation SQL are outside this code branch and require a separately logged rollout with rollback evidence.

---

### Task 1: Red-capable zero-network persistence benchmark

**Files:**
- Create: `scripts/research/bench_fbref_persistence.py`
- Create: `tests/unit/scripts/test_bench_fbref_persistence.py`
- Modify: `pyproject.toml`

**Interfaces:**
- Consumes: the ten tracked `tests/fixtures/fbref/matches/*.html.gz` files, `FBrefTypedBronzeWriter`, `FBrefGenericBronzeWriter`, and `TrinoTableManager`.
- Produces: `run_benchmark(config: BenchmarkConfig) -> BenchmarkReport`, JSON output containing sequential/batch timings, manager statement counts, per-table normalized digests, proxy requests/bytes (always zero), and gate verdicts.

- [ ] **Step 1: Write tests for the benchmark contract**

```python
def test_gate_fails_for_slow_or_non_equivalent_candidate():
    report = evaluate_gate(
        sequential_seconds=80.0,
        batch_seconds=30.0,
        matches=2,
        equivalent=False,
        proxy_requests=0,
        proxy_bytes=0,
    )
    assert report.speedup == pytest.approx(80.0 / 30.0)
    assert report.passed is False


def test_gate_requires_zero_network_and_four_x_speedup():
    report = evaluate_gate(
        sequential_seconds=80.0,
        batch_seconds=16.0,
        matches=2,
        equivalent=True,
        proxy_requests=0,
        proxy_bytes=0,
    )
    assert report.seconds_per_match == pytest.approx(8.0)
    assert report.passed is True
```

- [ ] **Step 2: Run the focused tests and confirm the missing-module failure**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/scripts/test_bench_fbref_persistence.py -q`

Expected: FAIL because `scripts.research.bench_fbref_persistence` does not exist.

- [ ] **Step 3: Implement the benchmark runner**

```python
@dataclass(frozen=True)
class BenchmarkConfig:
    html_dir: Path
    sequential_schema: str
    batch_schema: str | None
    iterations: int = 3
    min_speedup: float = 4.0
    max_seconds_per_match: float = 20.0


@dataclass(frozen=True)
class GateResult:
    speedup: float
    seconds_per_match: float
    equivalent: bool
    proxy_requests: int
    proxy_bytes: int
    passed: bool


def evaluate_gate(*, sequential_seconds: float, batch_seconds: float,
                  matches: int, equivalent: bool,
                  proxy_requests: int, proxy_bytes: int,
                  min_speedup: float = 4.0,
                  max_seconds_per_match: float = 20.0) -> GateResult:
    speedup = sequential_seconds / batch_seconds
    seconds_per_match = batch_seconds / matches
    return GateResult(
        speedup=speedup,
        seconds_per_match=seconds_per_match,
        equivalent=equivalent,
        proxy_requests=proxy_requests,
        proxy_bytes=proxy_bytes,
        passed=(equivalent and proxy_requests == 0 and proxy_bytes == 0
                and speedup >= min_speedup
                and seconds_per_match <= max_seconds_per_match),
    )
```

The CLI must accept `--html-dir`, `--sequential-schema`, optional `--batch-schema`, `--iterations`, `--output`, `--min-speedup`, and `--max-seconds-per-match`. Sequential mode must work before batch APIs exist; candidate mode must fail with a clear `batch persistence API is unavailable` message until Task 2 lands. Count calls to `_execute` and `_execute_committing` without logging SQL values. Normalize table rows by excluding only `_ingested_at` and `persisted_at`; retain `_batch_id`, `run_id`, payload, availability, and reason fields.

- [ ] **Step 4: Make the script importable in the test environment**

```toml
[tool.pytest.ini_options]
pythonpath = ["."]
```

Preserve every existing pytest option and marker in `pyproject.toml`; add `pythonpath` only if it is not already present.

- [ ] **Step 5: Run the benchmark unit tests**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/scripts/test_bench_fbref_persistence.py -q`

Expected: PASS.

- [ ] **Step 6: Commit the benchmark**

```bash
git add scripts/research/bench_fbref_persistence.py \
  tests/unit/scripts/test_bench_fbref_persistence.py pyproject.toml
git commit -m "test(fbref): add persistence throughput benchmark"
```

---

### Task 2: Batch generic and typed Bronze writers

**Files:**
- Modify: `scrapers/fbref/typed_bronze.py`
- Modify: `scrapers/fbref/bronze.py`
- Modify: `dags/utils/maintenance_tasks.py`
- Modify: `tests/unit/scrapers/test_fbref_typed_bronze.py`
- Modify: `tests/unit/scrapers/test_fbref_generic_bronze.py`
- Modify: `tests/unit/dags/test_maintenance_tasks.py`

**Interfaces:**
- Consumes: Task 1's benchmark and existing single-item writer semantics.
- Produces: `TypedMatchPersistItem`, `GenericPagePersistItem`, `FBrefTypedBronzeWriter.persist_matches(items)`, and `FBrefGenericBronzeWriter.persist_pages(items)`; single-item methods delegate to one-item batches.

- [ ] **Step 1: Add failing typed-batch tests**

```python
def test_persist_matches_writes_each_table_once_and_returns_per_item_counts():
    manager = FakeManager()
    writer = typed.FBrefTypedBronzeWriter(manager)
    items = [_typed_item("match-a"), _typed_item("match-b")]
    counts = writer.persist_matches(items)
    assert len(counts) == 2
    assert all("match_events" in item for item in counts)
    assert sum(call["table"] == "fbref_match_events" for call in manager.writes) == 1
    assert manager.writes[-1]["table"] == "fbref_dataset_availability"


def test_persist_matches_rejects_duplicate_match_or_target_identity():
    writer = typed.FBrefTypedBronzeWriter(FakeManager())
    with pytest.raises(typed.TypedBronzeBatchUnsupported):
        writer.persist_matches([_typed_item("match-a"), _typed_item("match-a")])
```

Add explicit cases for mixed write/clear/skip, tuple-scoped availability replacement, disjoint nullable columns, all-clear batches, and validation of the entire batch before the first manager write.

- [ ] **Step 2: Add failing generic-batch and janitor tests**

```python
def test_persist_pages_merges_cells_inventory_manifest_once_in_that_order():
    manager = FakeManager()
    writer = FBrefGenericBronzeWriter(manager)
    counts = writer.persist_pages([_page_item("a"), _page_item("b")])
    assert len(counts) == 2
    assert [call["table"] for call in manager.merges] == [
        "fbref_table_cells", "fbref_table_inventory", "fbref_page_manifest"
    ]


def test_batch_stage_names_are_recognized_as_age_guarded_fbref_stages():
    assert classify_fbref_staging_table(
        "fbref_table_cells__stg_batch_0123456789abcdef_c"
    ).owner_kind == "batch"
```

Add cases for duplicate merge keys, page parser errors, manifest-last order, per-item counts, and a retained batch stage that is never attributed to the first observation.

- [ ] **Step 3: Run focused tests and confirm failures**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers/test_fbref_typed_bronze.py tests/unit/scrapers/test_fbref_generic_bronze.py tests/unit/dags/test_maintenance_tasks.py -q`

Expected: FAIL on the missing batch APIs and batch-stage classification.

- [ ] **Step 4: Implement typed batching**

```python
@dataclass(frozen=True)
class TypedMatchPersistItem:
    parsed: MatchParseResult
    match_id: str
    context: TypedSourceContext
    run_id: str
    target_identity: str


class TypedBronzeBatchUnsupported(TypedBronzePersistenceError):
    """The cohort must use the already-claimed sequential fallback."""


def persist_matches(
    self, items: Sequence[TypedMatchPersistItem]
) -> list[Dict[str, int]]:
    materialized = tuple(items)
    if not materialized:
        return []
    validated = self._validate_match_batch(materialized)
    return self._persist_validated_match_batch(validated)
```

Implement `_validate_match_batch()` and `_persist_validated_match_batch()` in this task. Materialize and validate every item before the first write. Reject duplicate `match_id` or `target_identity`. For each dataset, replace only items whose action is `write` or `clear`; `skip` must not enter the delete scope. Decorate frames per item before concatenation, preserve nullable types, and call `insert_dataframe_atomic(self.schema, table, frame, delete_filter=delete_filter, single_statement_replace=True)` once per table. Availability must be the final Iceberg write and must delete exact `(match_id, dataset)` pairs rather than a Cartesian product. Return counts aligned with input items, including persisted datasets whose count is zero.

- [ ] **Step 5: Implement generic batching and batch-stage ownership**

```python
@dataclass(frozen=True)
class GenericPagePersistItem:
    page: PageDocument
    canonical_url: str
    run_id: str
    staging_identity: str


def persist_pages(
    self, items: Sequence[GenericPagePersistItem]
) -> list[dict[str, int]]:
    materialized = tuple(items)
    if not materialized:
        return []
    validated = self._validate_page_batch(materialized)
    return self._persist_validated_page_batch(validated)
```

Implement `_validate_page_batch()` and `_persist_validated_page_batch()` in this task. Prevalidate page errors and duplicate merge keys before writing. Concatenate decorated cells and inventory, then write cells, inventory, and manifests exactly once in that order. Derive a deterministic `batch_<16 hex>` owner from the sorted complete set of staging identities; never label a batch with the first observation. Extend maintenance classification so a failed batch stage is age-guarded and diagnosable but cannot be mistaken for an active single observation.

- [ ] **Step 6: Preserve single-item compatibility**

```python
def persist_match(self, parsed, *, match_id, context, run_id, target_identity):
    return self.persist_matches([TypedMatchPersistItem(
        parsed=parsed, match_id=match_id, context=context,
        run_id=run_id, target_identity=target_identity,
    )])[0]


def persist_page(self, page, *, canonical_url, run_id, staging_identity=None):
    identity = staging_identity or run_id
    return self.persist_pages([GenericPagePersistItem(
        page=page, canonical_url=canonical_url,
        run_id=run_id, staging_identity=identity,
    )])[0]
```

- [ ] **Step 7: Run focused tests**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers/test_fbref_typed_bronze.py tests/unit/scrapers/test_fbref_generic_bronze.py tests/unit/dags/test_maintenance_tasks.py -q`

Expected: PASS.

- [ ] **Step 8: Commit writer batching**

```bash
git add scrapers/fbref/typed_bronze.py scrapers/fbref/bronze.py \
  dags/utils/maintenance_tasks.py \
  tests/unit/scrapers/test_fbref_typed_bronze.py \
  tests/unit/scrapers/test_fbref_generic_bronze.py \
  tests/unit/dags/test_maintenance_tasks.py
git commit -m "perf(fbref): batch Bronze persistence by table"
```

---

### Task 3: Safe pipeline batching and zero-network fallback

**Files:**
- Modify: `scrapers/fbref/pipeline.py`
- Modify: `tests/unit/scrapers/test_fbref_pipeline.py`
- Modify: `compose.yaml`

**Interfaces:**
- Consumes: Task 2's writer batch items/APIs and existing `ObservationLease`, `guard_latest_content`, and raw-store records.
- Produces: `_process_claimed_observation` and `_process_claimed_match_batch`, plus environment-controlled match batching with the original sequential path as fallback.

- [ ] **Step 1: Add failing pipeline tests for batch success**

```python
def test_match_parse_wave_batches_generic_and_typed_writes_but_completes_each_lease():
    pipeline, observations = _pipeline_with_two_saved_matches()
    pipeline.batch_persist_enabled = True
    result = pipeline.parse_wave(
        RUN_ID, page_kinds=["match"], settings=_settings(shard_size=8)
    )
    assert pipeline.generic_writer.batch_sizes == [2]
    assert pipeline.typed_adapter.writer.batch_sizes == [2]
    assert result.parsed == 2
    assert [item.state for item in observations] == ["succeeded", "succeeded"]
```

Assert that availability is committed before `typed:__complete__`, and observation success is last. Add an A→B→A/latest race where a new fetch appears between the short probe and final locks; stale typed bytes must not replace current typed data.

- [ ] **Step 2: Add failing fallback and crash tests**

```python
def test_batch_failure_reuses_claimed_leases_and_never_fetches_network():
    pipeline, observations = _pipeline_with_two_saved_matches()
    pipeline.typed_adapter.writer.batch_error = RuntimeError("commit reset")
    result = pipeline.parse_wave(
        RUN_ID, page_kinds=["match"], settings=_settings(shard_size=8)
    )
    assert pipeline.control.claim_calls == len(observations)
    assert pipeline.fetcher_calls == 0
    assert result.parsed == 2
```

Add fault points after generic cells, inventory, manifest, each typed table, availability, per-dataset manifests, `typed:__complete__`, and observation completion. After replay, the final fake-table/control state must match sequential baseline; no premature completion marker and no permanent `processing` row are allowed.

- [ ] **Step 3: Run pipeline tests and confirm failures**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers/test_fbref_pipeline.py -q`

Expected: FAIL because batch orchestration is absent.

- [ ] **Step 4: Extract the already-claimed sequential helper**

```python
def _process_claimed_observation(
    self, *, run_id: str, html: str, record: RawFetchRecord,
    observation_lease: ObservationLease, page: PageDocument | None,
    typed_match: MatchParseResult | None, stateful_run_id: str,
    stateful_run_type: str,
) -> _ProcessedObservation:
    page = page or self._persist_generic(run_id, html, record)
    with self.control.guard_latest_content(
        record.target_id, record.content_hash, record.logical_refresh_id
    ) as is_latest:
        return self._finish_claimed_observation(
            run_id=run_id, html=html, record=record,
            observation_lease=observation_lease, page=page,
            typed_match=typed_match, is_latest=is_latest,
            stateful_run_id=stateful_run_id,
            stateful_run_type=stateful_run_type,
        )
```

Implement `_finish_claimed_observation()` in this task by moving the existing guarded typed/stateful/completion body without changing its order. The helper must never claim or fetch. It must preserve the existing order `generic → latest guard → typed → stateful → page completion → observation completion`. Its exception path records failure against the supplied lease and allows the caller to continue with other items.

- [ ] **Step 5: Implement two-phase match batching**

```python
def _process_claimed_match_batch(
    self, items: Sequence[_ClaimedMatchObservation]
) -> list[_ProcessedObservation]:
    materialized = tuple(items)
    if not materialized:
        return []
    if self._batch_has_duplicate_identity(materialized):
        return [self._process_claimed_observation(**item.sequential_args())
                for item in materialized]
    return self._persist_and_finish_match_batch(materialized)
```

Implement `_batch_has_duplicate_identity()`, `_ClaimedMatchObservation.sequential_args()`, and `_persist_and_finish_match_batch()` in this task. Phase one loads immutable raw HTML, claims once, parses generic and typed representations, and checks batch/cell limits. Duplicate target/match identities go directly to the already-claimed sequential helper. Phase two persists generic pages outside frontier locks, acquires unique target locks in sorted order with `ExitStack`, re-evaluates the complete latest verdict under those locks, persists only latest typed items as one batch, applies stateful effects, and writes completion markers per item before releasing locks. A leased target is deferred, never labelled stale or complete.

If either batch writer raises, retry each item through `_process_claimed_observation` using the same HTML, record, parsed values, and lease. Because writes are idempotent, partial table commits are repaired without network.

- [ ] **Step 6: Add bounded environment configuration**

```python
FBREF_BATCH_PERSIST = os.environ.get("FBREF_BATCH_PERSIST", "0") == "1"
FBREF_BATCH_PERSIST_MATCHES = _bounded_int(
    "FBREF_BATCH_PERSIST_MATCHES", default=8, lower=2, upper=25
)
FBREF_BATCH_PERSIST_MAX_CELLS = _bounded_int(
    "FBREF_BATCH_PERSIST_MAX_CELLS", default=150000,
    lower=1000, upper=500000,
)
```

Add the three variables to scheduler/webserver environment sections in `compose.yaml`, with batch persistence defaulting to `0`.

- [ ] **Step 7: Run pipeline and writer tests**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers/test_fbref_pipeline.py tests/unit/scrapers/test_fbref_typed_bronze.py tests/unit/scrapers/test_fbref_generic_bronze.py -q`

Expected: PASS.

- [ ] **Step 8: Commit pipeline integration**

```bash
git add scrapers/fbref/pipeline.py tests/unit/scrapers/test_fbref_pipeline.py compose.yaml
git commit -m "perf(fbref): persist match waves in safe batches"
```

---

### Task 4: Remove self-imposed orchestration stalls without removing safety

**Files:**
- Modify: `dags/utils/fbref_pipeline_tasks.py`
- Modify: `dags/utils/fbref_current_dag_factory.py`
- Modify: `dags/dag_backfill_fbref.py`
- Modify: `dags/dag_accept_fbref_bronze.py`
- Modify: `dags/scripts/run_fbref_live_waves.py`
- Modify: `scrapers/fbref/pipeline.py`
- Modify: `scrapers/fbref/control/store.py`
- Modify: `compose.yaml`
- Modify: `tests/unit/dags/test_fbref_pipeline_tasks.py`
- Modify: `tests/unit/dags/test_dag_bootstrap_fbref.py`
- Modify: `tests/unit/dags/test_dag_ingest_fbref.py`
- Modify: `tests/unit/dags/test_dag_backfill_fbref.py`
- Modify: `tests/unit/dags/test_fbref_bronze_acceptance_dags.py`
- Modify: `tests/unit/dags/test_run_fbref_live_waves_runner.py`
- Modify: `tests/unit/scrapers/test_fbref_pipeline.py`
- Modify: `tests/unit/scrapers/test_fbref_control_store_v8.py`

**Interfaces:**
- Consumes: durable progress/completion semantics from Tasks 2–3.
- Produces: a dedicated one-slot FBref Airflow pool, a six-hour child limit with an outer cleanup margin, capacity for 2,000 targets per run, honest run success classification, and a proven superseded-only recovery skip.

- [ ] **Step 1: Add failing timeout/pool tests**

```python
def test_bootstrap_has_eight_hour_dag_and_live_cleanup_margin():
    dag = load_bootstrap_dag()
    live = dag.get_task("run_live_waves")
    assert dag.dagrun_timeout == timedelta(hours=8)
    assert live.execution_timeout == timedelta(hours=6, minutes=5)
    assert live.pool == "fbref_scraper_pool"
    assert live.op_kwargs["max_batches"] == 80
```

Assert the same pool and outer task limit in current/backfill DAGs, move the paid live-acceptance task to the dedicated pool too, keep the internal subprocess wall clock at six hours, and make every wrapper/CLI/pipeline boundary accept exactly 1–80 batches. At shard size 25 this permits 2,000 targets in one run, enough headroom for the 1,500-match/day production gate once Task 5 removes the obsolete tariff profile.

- [ ] **Step 2: Add failing honest-success tests**

```python
def test_productive_run_warns_on_partial_warm_http_success():
    summary = _summary(requests=20, succeeded=19, durable_progress=19,
                       warm_http_success_rate=0.9494)
    result = validate_and_finish(summary)
    assert result["warnings"]["warm_http_success_rate"] == pytest.approx(0.9494)


def test_zero_success_remains_a_hard_failure():
    with pytest.raises(RunValidationError, match="zero warm HTTP successes"):
        validate_and_finish(_summary(requests=20, succeeded=0,
                                     warm_http_success_rate=0.0))
```

Keep unclassified failures, duplicate-fetch detection, and the driver queue-movement guard hard.

- [ ] **Step 3: Add failing superseded-recovery tests**

```python
def test_unprocessed_fetches_skip_only_failure_with_newer_complete_success():
    rows = store.list_unprocessed_fetches(
        parser_version=PAGE_VERSION,
        typed_parser_version=TYPED_VERSION,
        stateful_parser_version=STATEFUL_VERSION,
        page_kinds=["match"], limit=100,
    )
    assert [row["logical_refresh_id"] for row in rows] == [UNRESOLVED_REFRESH]
```

Cover unresolved failure, newer success with a different parser-version triple, A→B→A content, and an active processing lease. Never skip merely because a row is old or previously failed.

- [ ] **Step 4: Run focused tests and confirm failures**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/dags/test_fbref_pipeline_tasks.py tests/unit/dags/test_dag_bootstrap_fbref.py tests/unit/dags/test_dag_ingest_fbref.py tests/unit/dags/test_dag_backfill_fbref.py tests/unit/dags/test_fbref_bronze_acceptance_dags.py tests/unit/dags/test_run_fbref_live_waves_runner.py tests/unit/scrapers/test_fbref_pipeline.py tests/unit/scrapers/test_fbref_control_store_v8.py -q`

Expected: FAIL on current pool/timeouts, success thresholds, and poison-row selection.

- [ ] **Step 5: Align every timeout and batch ceiling**

```python
FBREF_LIVE_WAVE_WALL_CLOCK_TIMEOUT_SECONDS = 6 * 60 * 60
FBREF_MAX_LIVE_BATCHES = 80
FBREF_SCRAPER_POOL = "fbref_scraper_pool"
```

Set bootstrap `dagrun_timeout=8h`, live task `execution_timeout=6h05m`, and the internal subprocess timeout to six hours so SIGTERM/process-group cleanup has an outer margin. Use `fbref_scraper_pool` for FBref live tasks only, including live acceptance, keep its size one, and add idempotent pool creation to `airflow-init`. Do not increase the shared `ingest_scraper_pool`.

- [ ] **Step 6: Make success state-based rather than colour-based**

Treat zero warm successes after attempts, warm success below 0.5, no durable progress with claimed work, unclassified failures, and duplicate fetches as errors. Treat 0.5–0.95 warm success as a warning when durable progress exists and all failed claims were requeued/terminalized. Treat `promotion_pending` as an error only for an otherwise empty run; warn when productive work remains recoverable.

- [ ] **Step 7: Skip only provably superseded recovery rows**

Modify `list_unprocessed_fetches` and its count companion with the same `NOT EXISTS` predicate: a failed observation is terminal only when a newer observation for the same target has complete generic, typed, and stateful success for the exact requested parser-version triple. Keep active leases and unresolved errors visible.

- [ ] **Step 8: Run focused tests**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/dags/test_fbref_pipeline_tasks.py tests/unit/dags/test_dag_bootstrap_fbref.py tests/unit/dags/test_dag_ingest_fbref.py tests/unit/dags/test_dag_backfill_fbref.py tests/unit/dags/test_fbref_bronze_acceptance_dags.py tests/unit/dags/test_run_fbref_live_waves_runner.py tests/unit/scrapers/test_fbref_pipeline.py tests/unit/scrapers/test_fbref_control_store_v8.py -q`

Expected: PASS.

- [ ] **Step 9: Commit orchestration fixes**

```bash
git add dags/utils/fbref_pipeline_tasks.py dags/utils/fbref_current_dag_factory.py \
  dags/dag_backfill_fbref.py dags/dag_accept_fbref_bronze.py \
  dags/scripts/run_fbref_live_waves.py scrapers/fbref/pipeline.py \
  scrapers/fbref/control/store.py compose.yaml tests/unit/dags \
  tests/unit/scrapers/test_fbref_pipeline.py \
  tests/unit/scrapers/test_fbref_control_store_v8.py
git commit -m "fix(fbref): remove false stalls from productive runs"
```

---

### Task 5: Remove tariff caps and make Decodo session rotation deterministic

**Files:**
- Modify: `scrapers/fbref/settings.py`
- Modify: `scrapers/fbref/readiness.py`
- Modify: `scrapers/fbref/pipeline.py`
- Modify: `dags/utils/fbref_pipeline_tasks.py`
- Modify: `dags/utils/fbref_current_dag_factory.py`
- Modify: `dags/dag_backfill_fbref.py`
- Modify: `dags/scripts/run_fbref_live_waves.py`
- Modify: `scripts/proxy_filter/filter_proxy.py`
- Modify: `compose.yaml`
- Modify: `.env.example`
- Modify: `docs/operations/fbref-paid-transport.md`
- Modify: `docs/operations/sql/fbref_control_dataset_acceptance.sql`
- Modify: `docs/operations/sql/fbref_production_acceptance.sql`
- Modify: `tests/unit/dags/test_fbref_pipeline_tasks.py`
- Modify: `tests/unit/dags/test_dag_bootstrap_fbref.py`
- Modify: `tests/unit/dags/test_dag_ingest_fbref.py`
- Modify: `tests/unit/dags/test_dag_backfill_fbref.py`
- Modify: `tests/unit/dags/test_run_fbref_live_waves_runner.py`
- Modify: `tests/unit/scrapers/test_fbref_readiness.py`
- Modify: `tests/unit/scrapers/test_fbref_pipeline.py`
- Modify: `tests/unit/scripts/test_filter_proxy.py`
- Modify: `tests/unit/sql/test_fbref_production_acceptance_sql.py`
- Modify: `tests/integration/test_compose_validity.py`

**Interfaces:**
- Consumes: Task 4's 2,000-target run capacity and the existing lease/budget accounting.
- Produces: no tariff-sized production stop, a high explicit runaway circuit breaker, and deterministic no-immediate-repeat Decodo session selection after transport failure.

- [ ] **Step 1: Add failing capacity and breaker tests**

Assert that production uses `4096` requests and `2048 MiB` only as an emergency circuit breaker, while the non-publishing canary remains exactly `100/50`. Keep the stored `request_limit`/`byte_limit` fields for schema compatibility, but remove wording and validation that treats the production values as an acceptable tariff budget. Prove the worst-case request headroom: `80 * 25 * 2 + 4 * 20 == 4080 < 4096`. Direct CLI values must match one exact profile and the already-created control run before any fetcher/network construction. Production reaching the circuit exactly or exhausting it is a loud incomplete-run failure; canary exhaustion remains a clean non-publishing bounded stop.

- [ ] **Step 2: Add failing Decodo rotation tests**

For the dedicated FBref proxy-filter service, prove that a failed lease cannot immediately select the same normalized `(host casefolded, integer port, username)` session identity when another healthy identity exists. Selection must be deterministic and bounded, must never repin an active lease, and must not change the random-selection contract for other proxy-filter consumers. The safe fingerprint includes username/session identity but never password. Tests and errors may contain only that hash, never username, password, full proxy URL, or exit IP. A one-entry pool may reuse its identity only after the FBref cooldown; all-unhealthy returns a redacted `503 upstream_unavailable`, never the generic `429 budget` classification.

- [ ] **Step 3: Replace business profiles with safety controls**

```python
FBREF_PRODUCTION_SAFETY_REQUEST_LIMIT = 4096
FBREF_PRODUCTION_SAFETY_BYTE_LIMIT_MIB = 2048
FBREF_CANARY_REQUEST_LIMIT = 100
FBREF_CANARY_BYTE_LIMIT_MIB = 50
```

The production constants are circuit breakers, not success gates. Keep `DEFAULT_REQUEST_LIMIT`/`DEFAULT_BYTE_LIMIT` as compatibility aliases to the production safety values. Update the production replay-source profile too. A run that reaches either breaker fails loudly as a runaway/incomplete run; ordinary acceptance compares bytes per durable match without an absolute-MB pass threshold. Keep max response size, one active lease, 6.1-second interval, raw-first commit, and all reservation settlement checks. Readiness verifies that `min(daily, run, URL, max-lease)` configured capacity reaches the safety circuit; it still reconciles the provider counters but must not reject a capable filter merely because some of today's allowance is already spent.

- [ ] **Step 4: Align proxy-filter safety configuration**

Expose one secret-free `FBREF_PROXY_SAFETY_CIRCUIT_MIB` Compose setting, default `2048`, and use it for the dedicated FBref daily/run/URL and max-lease filter ceilings. Replace the four stale FBref cap variables in `.env.example`; do not increase limits for other scrapers or isolated `100/50` acceptance. Keep `--max-active-leases 1`. Production credentials remain only in the deployment-owned `0640` proxy file mounted into the proxy filter; Airflow workers continue to see only the local filter endpoint.

- [ ] **Step 5: Implement no-immediate-repeat session rotation**

Track a bounded FBref-only cursor/last failed normalized session identity in the filter's lease allocator. On a new lease after failure, scan at most the unique candidate count, exclude the prior identity when another healthy candidate exists, and fail safely if none is healthy. Record CONNECT and direct-dial failures before rethrowing. Do not call the shared host/port-only `ProxyManager.record_result`, do not silently switch upstream inside an active lease, and leave non-FBref random selection untouched. In-memory no-repeat state may reset with the filter process; document that residual rather than claiming cross-restart exclusion.

- [ ] **Step 6: Run focused tests**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/dags/test_fbref_pipeline_tasks.py tests/unit/dags/test_dag_bootstrap_fbref.py tests/unit/dags/test_dag_ingest_fbref.py tests/unit/dags/test_dag_backfill_fbref.py tests/unit/dags/test_run_fbref_live_waves_runner.py tests/unit/scrapers/test_fbref_readiness.py tests/unit/scrapers/test_fbref_pipeline.py tests/unit/scripts/test_filter_proxy.py tests/unit/sql/test_fbref_production_acceptance_sql.py tests/integration/test_compose_validity.py -q`

Expected: PASS with no credential-bearing output.

- [ ] **Step 7: Commit capacity and rotation changes**

```bash
git add scrapers/fbref/settings.py scrapers/fbref/readiness.py \
  dags/utils/fbref_pipeline_tasks.py dags/utils/fbref_current_dag_factory.py \
  dags/dag_backfill_fbref.py dags/scripts/run_fbref_live_waves.py \
  scripts/proxy_filter/filter_proxy.py compose.yaml tests
git commit -m "perf(fbref): replace tariff caps with safety circuit"
```

---

### Task 6: Reuse the metered Decodo HTTP session across pages

**Files:**
- Modify: `scrapers/fbref/proxy_lease.py`
- Modify: `scrapers/fbref/fetcher.py`
- Modify: `scrapers/fbref/pipeline.py`
- Modify: `scrapers/fbref/control/store.py`
- Modify: `scripts/proxy_filter/filter_proxy.py`
- Modify: `compose.yaml`
- Modify: `tests/unit/scrapers/test_fbref_proxy_lease.py`
- Modify: `tests/unit/scrapers/test_fbref_metered_fetcher.py`
- Modify: `tests/unit/scrapers/test_fbref_pipeline.py`
- Modify: `tests/unit/scrapers/test_fbref_control_store_v8.py`
- Modify: `tests/unit/scripts/test_filter_proxy.py`

**Interfaces:**
- Consumes: the existing serial `curl_cffi.Session`, authoritative proxy-filter counters, and Task 5's safety circuit.
- Produces: optional persistent HTTP/TLS reuse, exact per-page plus final-tail accounting, and unchanged raw/data/retry semantics.

- [ ] **Step 1: Add failing persistent-session tests**

Behind `FBREF_PERSISTENT_HTTP_SESSION=0` by default, two successful pages must use one curl session and one pinned proxy lease, must not close after page one, and must close exactly once at final session shutdown. A 403/429, dead clearance, transport failure, meter ambiguity, or explicit rollover still closes and rotates. Conditional headers, decoded/raw bytes, content hashes, and the raw-before-settlement order must remain unchanged.

- [ ] **Step 2: Add failing exact-accounting tests**

Add non-secret proxy-filter stats for `active_provider_readers`, `provider_reserved_bytes`, and `pending_client_hellos`. Add a proxy-lease `wait_idle()` checkpoint and require two identical authoritative samples. With zero tunnels every reservation must be zero. With one expected tunnel, exactly one provider reader may exist, `reserved_bytes == provider_reserved_bytes`, and there must be no pending client hello. Any other reservation cannot be proven idle and fails closed. Prove:

```text
sum(page provider deltas) + final connection tail == authoritative close total
```

The control-plane run byte total and clearance-session metric must equal the same value exactly. Counter regression, timeout, unexpected tunnels, unknown final tail, or insufficient tail reservation fails closed and opens no new paid lease.

- [ ] **Step 3: Implement persistent metered checkpoints**

Replace success-path per-page close/drain with the new serial idle checkpoint. Keep the current close/drain behavior for all rotation/error paths. Do not create parallel requests: one session remains strictly serial and the source interval still gates every target.

- [ ] **Step 4: Settle final connection overhead idempotently**

Create a run-scoped, zero-request session-tail reservation keyed deterministically from the non-secret clearance `session_id` before opening the persistent connection; reserve the existing conservative per-target byte amount. Add one atomic `settle_clearance_session_tail()` control API which locks run, reservation, and clearance session in that order, proves `page provider sum + tail == authoritative close total`, settles the reservation, sets the session total absolutely, and stores idempotency evidence. Identical repeats are no-ops; a different repeat conflicts. On `_LiveFetchSession.close()`, close curl and the provider lease, settle the exact tail, then close the control clearance session. An orphan tail blocks a new paid lease and is conservatively charged by run abort. Never attribute tail bytes to an arbitrary page or leave them uncounted.

- [ ] **Step 5: Preserve zero-network recovery**

Any failure after immutable raw commit retries parsing/persistence only from raw. A healthy target never rotates merely to obtain a new IP. A rollover may happen only on a raw boundary and must settle the old session completely before opening the next lease. Fix the adjacent response-obtained/raw-store-failure path to settle authoritative `provider_billed_bytes` and session metrics rather than only local wire bytes.

- [ ] **Step 6: Run focused tests**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers/test_fbref_proxy_lease.py tests/unit/scrapers/test_fbref_metered_fetcher.py tests/unit/scrapers/test_fbref_pipeline.py tests/unit/scrapers/test_fbref_control_store_v8.py tests/unit/scripts/test_filter_proxy.py -q`

Expected: PASS, including the pre-existing hard-transport-policy and raw-first ordering tests.

- [ ] **Step 7: Commit persistent-session support**

```bash
git add scrapers/fbref/proxy_lease.py scrapers/fbref/fetcher.py \
  scrapers/fbref/pipeline.py scrapers/fbref/control/store.py \
  scripts/proxy_filter/filter_proxy.py compose.yaml tests/unit/scrapers \
  tests/unit/scripts/test_filter_proxy.py
git commit -m "perf(fbref): reuse metered proxy sessions across pages"
```

---

### Task 7: Decodo-efficient rollout evidence and completeness gate

**Files:**
- Create: `scripts/research/bench_fbref_decodo_session.py`
- Create: `tests/unit/scripts/test_bench_fbref_decodo_session.py`
- Create: `docs/operations/sql/fbref_male_completeness_control.sql`
- Create: `docs/operations/fbref-decodo-rollout.md`
- Modify: `scripts/research/bench_fbref_persistence.py`
- Modify: `tests/unit/scripts/test_bench_fbref_persistence.py`

**Interfaces:**
- Consumes: Tasks 1–6 and Decodo credentials supplied only through the runtime proxy file.
- Produces: a no-secret sticky-session canary, canonical completeness SQL, bidirectional 12-table replay comparison, and an explicit staged rollout/rollback runbook.

- [ ] **Step 1: Add failing session-canary tests**

```python
def test_exit_identity_is_hashed_and_credentials_never_enter_report():
    report = build_probe_report(
        proxy_url="http://user:secret@gate.decodo.com:7000",
        observed_ip="203.0.113.10",
        elapsed_seconds=3600,
    )
    rendered = json.dumps(report)
    assert "secret" not in rendered
    assert "203.0.113.10" not in rendered
    assert report["exit_hash"] == hashlib.sha256(
        b"203.0.113.10"
    ).hexdigest()
```

Cover probes at 0/10/30/60/120 minutes, a gap over 60 seconds, provider-counter lag, and report redaction.

- [ ] **Step 2: Implement the bounded Decodo canary**

```python
PROBE_OFFSETS_SECONDS = (0, 600, 1800, 3600, 7200)


def hash_exit_identity(ip: str) -> str:
    return hashlib.sha256(ip.encode("ascii")).hexdigest()
```

The canary reads exactly one proxy URL from a file, never prints it, performs only the configured IP-check probes, and records timestamps plus exit hashes. It must not contact FBref. The rollout runbook selects the shortest custom sticky duration that remains stable across the longest uninterrupted fetch window, with a minimum candidate of 60 minutes; transport failures still trigger rotation.

- [ ] **Step 3: Add canonical completeness SQL**

```sql
WITH latest_schedule AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY regexp_extract(match_url, '/matches/([^/]+)', 1)
               ORDER BY _ingested_at DESC
           ) AS rn
    FROM iceberg.bronze.fbref_schedule
    WHERE source_season_id IN ('2025-2026', '2026-2027')
),
played AS (
    SELECT * FROM latest_schedule
    WHERE rn = 1 AND score IS NOT NULL AND trim(score) <> ''
)
SELECT source_competition_id, source_season_id,
       count(*) AS played_matches,
       count(e.match_id) AS matches_with_events,
       count(*) - count(e.match_id) AS missing_matches
FROM played s
LEFT JOIN iceberg.bronze.fbref_match_events e
  ON e.match_id = regexp_extract(s.match_url, '/matches/([^/]+)', 1)
GROUP BY 1, 2
ORDER BY 1, 2;
```

Adapt column names only to the installed Bronze schema and retain match-key deduplication, male-registry scope, both source seasons, per-competition rows, and a total row. Add comments defining legitimately empty/dead competitions.

- [ ] **Step 4: Complete the differential benchmark**

For every one of the 12 tables, compare sequential and batch schemas with `EXCEPT ALL` in both directions after excluding only volatile timestamps. Compare control-plane observation/manifests/latest-state output per logical refresh. Add sentinels outside the cohort to prove no collateral delete. The JSON report must include per-table left/right diff counts, elapsed seconds, Trino statements, Iceberg snapshot deltas, proxy requests/bytes, speedup, seconds per match, and one final pass/fail field.

- [ ] **Step 5: Write staged rollout and rollback instructions**

The runbook sequence is: rotate exposed credentials; create a dedicated Decodo sub-user; run the non-FBref exit canary; create a 060/90/120-minute custom-sticky candidate file with mode `0640`; run sequential and batch replay with zero network; enable `FBREF_BATCH_PERSIST=1` for replay only; require 12-table/control-plane equivalence and the performance gate; run one bounded daily canary with unchanged request limits; reconcile Decodo dashboard bytes against the local paid ledger; enable daily; then enable drain. Rollback is feature flag off plus the previous proxy file/image digest. Never include username, password, or full proxy URLs in the document or evidence.

- [ ] **Step 6: Run focused tests**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/scripts/test_bench_fbref_persistence.py tests/unit/scripts/test_bench_fbref_decodo_session.py tests/unit/sql -q`

Expected: PASS.

- [ ] **Step 7: Run the complete unit suite**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit -q`

Expected: PASS with only the repository's documented Airflow-dependent skips.

- [ ] **Step 8: Commit acceptance tooling and documentation**

```bash
git add scripts/research/bench_fbref_decodo_session.py \
  scripts/research/bench_fbref_persistence.py \
  tests/unit/scripts/test_bench_fbref_decodo_session.py \
  tests/unit/scripts/test_bench_fbref_persistence.py \
  docs/operations/sql/fbref_male_completeness_control.sql \
  docs/operations/fbref-decodo-rollout.md
git commit -m "docs(fbref): add Decodo rollout and completeness gates"
```

---

## Final verification

- [ ] Run `git diff --check origin/master...HEAD` and require no output.
- [ ] Run `/root/.venvs/dpf-test/bin/pytest tests/unit -q` and record the exact pass/skip count.
- [ ] Run the sequential benchmark once and confirm it is red-capable against the `4×`/`20s` gate.
- [ ] Run the batch benchmark in fresh isolated schemas and require bidirectional equivalence, zero proxy deltas, at least `4×`, and no more than `20s/match`.
- [ ] Obtain a whole-branch review focused on batch failure semantics, latest-content locking, tuple-scoped deletes, and credential redaction.
- [ ] Open a draft PR referencing `#1145`; do not deploy from the development worktree.
