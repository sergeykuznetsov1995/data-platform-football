"""Offline SofaScore raw-replay worker (#1367).

These helpers normalize one exact replay scope, resolve it through the
tournament catalog, select the planner's deterministic raw-replay plan and
rebuild exact season endpoint specs from already-captured manifest records —
all network-free.  The match, player and season phases republish captured
raw into Bronze with zero source traffic and the strict CLI below drives one
exact scope end to end.  DAG wiring deliberately stays out of this module.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from collections import Counter
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
)

from scrapers.sofascore.catalog import SofaScoreCatalog
from scrapers.sofascore.manifest import (
    EndpointManifest,
    JsonFileManifestStore,
    ManifestKey,
    ManifestStatus,
)
from scrapers.sofascore.raw_replay import (
    MODES,
    ReplayManagerProtocol,
    ReplayScopeKey,
    ReplayScopePlan,
    ResolvedReplayScope,
    load_replay_scope_records,
    resolve_replay_scope,
    select_replay_scope_plans,
)

if TYPE_CHECKING:
    from scrapers.sofascore.season_pipeline import (
        EndpointSpec,
        SeasonPartitionPlan,
    )

_SCOPE_FIELDS = ("source_tournament_id", "source_season_id", "freshness_key")

_SEASON_SPEC_ENDPOINTS = frozenset(
    {
        "standings_total",
        "rounds",
        "cup_trees",
        "participants",
        "referee_profile",
        "schedule_last",
        "schedule_next",
        "squads",
    }
)

_REPLAY_ENTITY = "sofascore_raw_replay"

_DEFAULT_REPLAY_OUTPUT = "/tmp/sofascore_replay_result.json"

_REPLAY_HISTORY_IDS_ENV = "SOFASCORE_HISTORY_MATCH_IDS_JSON"
_REPLAY_PHASE_ENV = "SOFASCORE_HISTORY_PHASE"
_REPLAY_EVIDENCE_ENV = "SOFASCORE_HISTORY_SEASON_EVIDENCE"
_REPLAY_PLAYER_IDS_ENV = "SOFASCORE_OFFLINE_PLAYER_IDS_JSON"
_REPLAY_PLAYER_FRESHNESS_ENV = "SOFASCORE_PLAYER_FRESHNESS_KEY"
_REPLAY_SEASON_FRESHNESS_ENV = "SOFASCORE_SEASON_FRESHNESS_KEY"

_MATCH_ENDPOINTS = (
    "event",
    "lineups",
    "statistics",
    "shotmap",
    "incidents",
)


class ReplayScopeError(ValueError):
    """A replay scope, its records or its capture runtime are unusable."""


def _normalize_scope(value: object) -> ReplayScopeKey:
    """Normalize a scope into a :class:`ReplayScopeKey` of stripped strings.

    Accepts a ``ReplayScopeKey``, a mapping with exactly
    ``source_tournament_id``/``source_season_id``/``freshness_key`` or a
    3-element sequence.  Booleans, empty values, wrong types and extra or
    missing mapping fields raise :class:`ReplayScopeError`.
    """
    if isinstance(value, ReplayScopeKey):
        values: Sequence[object] = tuple(value)
    elif isinstance(value, Mapping):
        fields = set(value)
        expected = set(_SCOPE_FIELDS)
        if fields != expected:
            raise ReplayScopeError(
                "scope mapping must contain exactly (source_tournament_id, "
                "source_season_id, freshness_key); "
                f"missing={sorted(expected - fields)}, "
                f"extra={sorted(fields - expected)}"
            )
        values = [value[name] for name in _SCOPE_FIELDS]
    elif isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ReplayScopeError(
            "scope must be a ReplayScopeKey, a mapping with exactly "
            "(source_tournament_id, source_season_id, freshness_key) or a "
            "(source_tournament_id, source_season_id, freshness_key) triple"
        )
    else:
        values = tuple(value)
    if len(values) != 3:
        raise ReplayScopeError(
            "scope must contain exactly (source_tournament_id, "
            f"source_season_id, freshness_key), got {len(values)} values"
        )
    normalized: list[str] = []
    for name, item in zip(_SCOPE_FIELDS, values):
        if isinstance(item, bool) or not isinstance(item, (str, int)):
            raise ReplayScopeError(
                f"{name} must be a str or int, got {item!r}"
            )
        text = str(item).strip()
        if not text:
            raise ReplayScopeError(f"{name} must not be empty")
        normalized.append(text)
    return ReplayScopeKey(*normalized)


def _replay_manager(manifest_store: object) -> ReplayManagerProtocol:
    """Unwrap a (possibly batched) manifest store to its trino manager."""
    store = manifest_store
    while True:
        inner = getattr(store, "inner", None)
        if inner is None or inner is store:
            break
        store = inner
    manager = getattr(store, "manager", None)
    if not callable(getattr(manager, "_execute", None)):
        raise ReplayScopeError(
            "manifest store does not expose its trino backend: expected the "
            "trino manifest store (optionally wrapped by a batching store) "
            "whose manager provides a callable _execute()"
        )
    return manager


def _load_scope(
    scope: object,
    *,
    mode: str,
    capture_runtime: object,
    records: Optional[Sequence[EndpointManifest]] = None,
    target_cap: int = 25,
) -> tuple[ResolvedReplayScope, Optional[ReplayScopePlan]]:
    """Resolve one exact replay scope and select its raw-replay plan.

    The catalog comes from the runtime when it carries one and otherwise from
    ``SofaScoreCatalog.load``.  Records already in memory are prefiltered to
    the exact scope triple; when absent they are loaded through the manifest
    store's trino manager with the planner's mode filters.  Returns the
    catalog-resolved scope and the single selected plan — selected with the
    caller's ``target_cap`` (default 25) — or ``None`` when nothing is
    replayable.
    """
    if mode not in MODES:
        raise ReplayScopeError(f"mode must be one of {MODES}, got {mode!r}")
    scope = _normalize_scope(scope)
    catalog = getattr(capture_runtime, "catalog", None)
    if catalog is None:
        catalog = SofaScoreCatalog.load(
            getattr(capture_runtime, "registry_path", None)
        )
    resolved = resolve_replay_scope(scope, catalog)
    if records is None:
        records = load_replay_scope_records(
            _replay_manager(getattr(capture_runtime, "manifest_store", None)),
            scope,
            mode=mode,
        )
    triple = (
        scope.source_tournament_id,
        scope.source_season_id,
        scope.freshness_key,
    )
    exact_records = tuple(
        record
        for record in records
        if (
            record.key.source_tournament_id,
            record.key.source_season_id,
            record.key.freshness_key,
        )
        == triple
    )
    selection = select_replay_scope_plans(
        exact_records,
        mode=mode,
        scope_cap=1,
        target_cap=target_cap,
        exact_scope=(
            scope.source_tournament_id,
            scope.source_season_id,
        ),
    )
    return resolved, (selection[0] if selection else None)


def _target_ids(
    records: Iterable[EndpointManifest], endpoint: str
) -> tuple[str, ...]:
    """Distinct target ids of one exact endpoint, sorted numerically."""
    return tuple(
        sorted(
            {
                record.key.target_id
                for record in records
                if record.key.endpoint == endpoint
            },
            key=int,
        )
    )


def _season_spec_for(record: EndpointManifest) -> "EndpointSpec":
    """Build the exact raw/replay spec one captured record describes.

    The season-pipeline builders are imported locally so importing this
    worker never pulls the capture engine.  Common kwargs come from
    ``record.key`` with ``paid_proxy=True``; every other endpoint raises
    :class:`ReplayScopeError`.
    """
    from scrapers.sofascore.season_pipeline import (
        build_cup_trees_spec,
        build_participants_spec,
        build_referee_profile_spec,
        build_rounds_spec,
        build_schedule_page_spec,
        build_squad_spec,
        build_standings_total_spec,
    )

    key = record.key
    common = {
        "source_tournament_id": key.source_tournament_id,
        "source_season_id": key.source_season_id,
        "freshness_key": key.freshness_key,
        "paid_proxy": True,
    }
    endpoint = key.endpoint
    try:
        if endpoint == "standings_total":
            spec = build_standings_total_spec(**common)
        elif endpoint == "rounds":
            spec = build_rounds_spec(**common)
        elif endpoint == "cup_trees":
            spec = build_cup_trees_spec(**common)
        elif endpoint == "participants":
            spec = build_participants_spec(**common)
        elif endpoint == "referee_profile":
            spec = build_referee_profile_spec(
                referee_id=key.target_id, expected_slugs=(), **common
            )
        elif endpoint in ("schedule_last", "schedule_next"):
            direction, _, page_token = key.target_id.partition(":")
            if endpoint != f"schedule_{direction}" or not page_token.isdigit():
                raise ReplayScopeError(
                    "schedule target_id must be '<last|next>:<page>', got "
                    f"{key.target_id!r} for endpoint {endpoint!r}"
                )
            spec = build_schedule_page_spec(
                direction=direction, page=int(page_token), **common
            )
        elif endpoint == "squads":
            spec = build_squad_spec(team_id=key.target_id, **common)
        else:
            raise ReplayScopeError(
                f"endpoint {endpoint!r} has no replay spec builder"
            )
    except ReplayScopeError:
        raise
    except ValueError as exc:
        raise ReplayScopeError(
            f"cannot build replay spec for endpoint {endpoint!r} target "
            f"{key.target_id!r}: {exc}"
        ) from exc
    if spec.key != key:
        raise ReplayScopeError(
            f"replay spec key mismatch for endpoint {endpoint!r}: built "
            f"{spec.key}, captured {key}"
        )
    return spec


def _season_plan(
    records: Sequence[EndpointManifest], scope: object
) -> "SeasonPartitionPlan":
    """Turn one exact scope's records into a replay season partition plan."""
    from scrapers.sofascore.season_pipeline import SeasonPartitionPlan

    scope = _normalize_scope(scope)
    triple = (
        scope.source_tournament_id,
        scope.source_season_id,
        scope.freshness_key,
    )
    specs: list["EndpointSpec"] = []
    seen: set[ManifestKey] = set()
    pending: list[ManifestKey] = []
    for record in records:
        key = record.key
        if (
            key.source_tournament_id,
            key.source_season_id,
            key.freshness_key,
        ) != triple:
            raise ReplayScopeError(
                f"record {key.stable_id()} is outside the replay scope "
                f"{triple}"
            )
        spec = _season_spec_for(record)
        if spec.key in seen:
            raise ReplayScopeError(
                "replay season plan contains duplicate manifest key "
                f"{spec.key.stable_id()}"
            )
        seen.add(spec.key)
        specs.append(spec)
        pending.append(key)
    return SeasonPartitionPlan(
        source_tournament_id=scope.source_tournament_id,
        source_season_id=scope.source_season_id,
        freshness_key=scope.freshness_key,
        event_freshness_key="final",
        specs=tuple(specs),
        pending_keys=tuple(pending),
        missing_raw_keys=(),
        schedule_event_ids=(),
        team_ids=_target_ids(records, "squads"),
        referee_ids=_target_ids(records, "referee_profile"),
    )


@contextmanager
def _env_overrides(values: Mapping[str, object]) -> Iterator[None]:
    """Temporarily apply env overrides and restore them in ``finally``.

    Each mapping entry is set to ``str(value)`` (or popped when the value
    is ``None``); on exit previously unset names are popped and previously
    set names are restored to their original value.
    """
    saved: dict[str, Optional[str]] = {}
    try:
        for name, value in values.items():
            saved[name] = os.environ.get(name)
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = str(value)
        yield
    finally:
        for name, previous in saved.items():
            if previous is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous


def _matches_phase(
    resolved: ResolvedReplayScope,
    records: Sequence[EndpointManifest],
    capture_runtime: object,
    *,
    mode: Optional[str] = None,
) -> dict[str, Any]:
    """Run the offline match phase for one exact replay scope.

    Fields mode replays ``lineups`` targets; drain mode replays the union
    of target ids across the five canonical event endpoints, so a match
    whose only pending record is a non-``event`` endpoint still replays
    from its already-captured raw.  The phase refuses to run without
    targets or when the scope freshness key is not ``final``.  The runner
    writes its JSON report inside a temporary directory and the report is
    read back defensively.
    """
    if mode is None:
        mode = "fields" if _target_ids(records, "lineups") else "drain"
    if mode == "fields":
        ids = _target_ids(records, "lineups")
    else:
        ids = tuple(
            sorted(
                {
                    target_id
                    for endpoint in _MATCH_ENDPOINTS
                    for target_id in _target_ids(records, endpoint)
                },
                key=int,
            )
        )
    if not ids:
        return {
            "ran": False,
            "reason": "no_targets",
            "targets": [],
            "errors": [],
        }
    freshness = getattr(resolved, "freshness_key", None)
    if freshness is None:
        freshness = getattr(
            getattr(resolved, "scope", None), "freshness_key", None
        )
    if freshness is None and records:
        freshness = records[0].key.freshness_key
    if freshness != "final":
        return {
            "ran": False,
            "reason": "freshness_key_not_final",
            "targets": [],
            "errors": [],
        }
    from dags.scripts import run_sofascore_scraper as runner

    with tempfile.TemporaryDirectory() as tmpdir:
        report_path = os.path.join(tmpdir, "matches_report.json")
        with _env_overrides(
            {
                _REPLAY_HISTORY_IDS_ENV: json.dumps(
                    list(ids), ensure_ascii=False
                ),
                _REPLAY_PHASE_ENV: "matches",
                _REPLAY_EVIDENCE_ENV: "bronze",
            }
        ):
            exit_code = runner._run_match_capture(
                [resolved.league],
                resolved.canonical_season,
                None,
                report_path,
                capture_runtime=capture_runtime,
                workload_plan=None,
                workload_allocations=(),
                force_replace=False,
                offline_replay=True,
            )
        report: dict[str, Any] = {}
        try:
            loaded: object = None
            if os.path.isdir(report_path):
                entries = sorted(
                    name
                    for name in os.listdir(report_path)
                    if name.endswith(".json")
                )
                if entries:
                    with open(
                        os.path.join(report_path, entries[0]),
                        encoding="utf-8",
                    ) as handle:
                        loaded = json.load(handle)
            else:
                with open(report_path, encoding="utf-8") as handle:
                    loaded = json.load(handle)
            if isinstance(loaded, Mapping):
                report = dict(loaded)
        except (OSError, ValueError):
            report = {}
    return {
        "ran": True,
        "exit_code": exit_code,
        "targets": list(ids),
        "report": report,
        "errors": report.get("errors", []),
    }


def _players_phase(
    resolved: ResolvedReplayScope,
    records: Sequence[EndpointManifest],
    capture_runtime: object,
    *,
    mode: Optional[str] = None,
) -> dict[str, Any]:
    """Run the offline player phase for one exact replay scope.

    Fields mode skips players entirely.  Drain mode collects the distinct
    player ids behind ``player_profile``/``player_season_statistics``
    records and publishes them together with the scope's exact freshness
    key through env overrides — both as the player freshness and as
    ``SOFASCORE_SEASON_FRESHNESS_KEY``, so the canonical
    ``plan_season_partition`` inside the player runner resolves the same
    season raw — and reads the runner's JSON report back defensively.
    """
    if mode == "fields":
        return {
            "ran": False,
            "reason": "fields_mode_skips_players",
            "targets": [],
            "errors": [],
        }
    ids = tuple(
        sorted(
            set(_target_ids(records, "player_profile"))
            | set(_target_ids(records, "player_season_statistics")),
            key=int,
        )
    )
    if not ids:
        return {
            "ran": False,
            "reason": "no_targets",
            "targets": [],
            "errors": [],
        }
    freshness = getattr(resolved, "freshness_key", None)
    if freshness is None:
        freshness = getattr(
            getattr(resolved, "scope", None), "freshness_key", None
        )
    if freshness is None and records:
        freshness = records[0].key.freshness_key
    from dags.scripts import run_sofascore_scraper as runner

    with tempfile.TemporaryDirectory() as tmpdir:
        report_path = os.path.join(tmpdir, "players_report.json")
        with _env_overrides(
            {
                _REPLAY_PLAYER_IDS_ENV: json.dumps(
                    list(ids), ensure_ascii=False
                ),
                _REPLAY_PLAYER_FRESHNESS_ENV: freshness,
                _REPLAY_SEASON_FRESHNESS_ENV: freshness,
            }
        ):
            exit_code = runner._run_player_capture(
                [resolved.league],
                resolved.canonical_season,
                None,
                report_path,
                capture_runtime=capture_runtime,
                workload_plan=None,
                workload_allocations=(),
                force_replace=False,
                offline_replay=True,
            )
        report = {}
        try:
            loaded = None
            if os.path.isdir(report_path):
                entries = sorted(
                    name
                    for name in os.listdir(report_path)
                    if name.endswith(".json")
                )
                if entries:
                    with open(
                        os.path.join(report_path, entries[0]),
                        encoding="utf-8",
                    ) as handle:
                        loaded = json.load(handle)
            else:
                with open(report_path, encoding="utf-8") as handle:
                    loaded = json.load(handle)
            if isinstance(loaded, Mapping):
                report = dict(loaded)
        except (OSError, ValueError):
            report = {}
    return {
        "ran": True,
        "exit_code": exit_code,
        "targets": list(ids),
        "report": report,
        "errors": report.get("errors", []),
    }


def _season_phase(
    resolved: ResolvedReplayScope,
    records: Sequence[EndpointManifest],
    scope: object,
    capture_runtime: object,
) -> dict[str, Any]:
    """Run the offline season phase for one exact replay scope.

    Rebuilds the exact season endpoint specs through :func:`_season_plan`,
    replays them with ``replay_season_specs`` and materializes the partition.
    Schedule and standings plus the normalized season fields republish inside
    one ``SofaScoreScraper``/``bronze_writer_lock`` span;
    ``finalize_materialized_results`` runs only after the lock released and
    the manifest store is flushed right after it.  There is no
    ``promote_repaired_results`` and no manual success upsert, a schedule
    chain or ``match_phase_ready`` is never required, and true raw-only
    squads replay without producing a table.  A phase error is reported, not
    hidden: without the finalize the records stay deferred/schema_error.
    """
    import pandas as pd
    from scrapers.sofascore import SofaScoreScraper
    from scrapers.sofascore.pipeline import finalize_materialized_results
    from scrapers.sofascore.season_pipeline import (
        materialize_season_partition,
        replay_season_specs,
    )
    from scrapers.sofascore.writer_lock import bronze_writer_lock

    from dags.scripts import run_sofascore_scraper as runner

    season_records = [
        record
        for record in records
        if record.key.endpoint in _SEASON_SPEC_ENDPOINTS
    ]
    if not season_records:
        return {
            "ran": False,
            "reason": "no_targets",
            "targets": [],
            "report": {},
            "errors": [],
        }
    targets = sorted({record.key.target_id for record in season_records})
    report: dict[str, Any] = {
        "entity": "sofascore_season_replay",
        "tables": [],
        "publication_receipts": [],
        "rows": 0,
        "schedule_rows": 0,
        "league_table_rows": 0,
        "planned_endpoints": 0,
        "errors": [],
    }
    try:
        plan = _season_plan(season_records, scope)
        report["planned_endpoints"] = len(plan.specs)
        replayed = replay_season_specs(capture_runtime.engine, plan.specs)
        materialized = materialize_season_partition(
            plan,
            replayed,
            canonical_league=resolved.league,
            canonical_season=resolved.canonical_season,
        )
        with SofaScoreScraper(
            leagues=[resolved.league],
            seasons=[resolved.canonical_season],
        ) as scraper, bronze_writer_lock():
            if materialized.schedule_rows:
                schedule_df = scraper._add_metadata(
                    pd.DataFrame(materialized.schedule_rows), "schedule"
                )
                path = runner._save_publication(
                    scraper,
                    report,
                    df=schedule_df,
                    table_name="sofascore_schedule",
                    partition_cols=["league", "season"],
                    natural_keys=["league", "season", "game_id"],
                )
                report["tables"].append(path)
                report["schedule_rows"] = len(schedule_df)
            if materialized.standings_rows:
                standings_df = scraper._add_metadata(
                    pd.DataFrame(materialized.standings_rows), "league_table"
                )
                path = runner._save_publication(
                    scraper,
                    report,
                    df=standings_df,
                    table_name="sofascore_league_table",
                    partition_cols=["league", "season"],
                    natural_keys=["league", "season", "group", "team"],
                )
                report["tables"].append(path)
                report["league_table_rows"] = len(standings_df)
            runner._publish_normalized_season_fields(
                scraper, report, materialized
            )
        finalize_materialized_results(capture_runtime, replayed)
        runner._flush_manifest_store(capture_runtime.manifest_store)
    except Exception as exc:
        report["errors"].append(f"season_phase: {exc}")
        return {
            "ran": False,
            "reason": "season_phase_error",
            "targets": targets,
            "report": report,
            "errors": list(report["errors"]),
        }
    return {
        "ran": True,
        "targets": targets,
        "report": report,
        "errors": [],
    }


def _merge_phase_report(
    results: MutableMapping[str, Any],
    phase: str,
    phase_result: Mapping[str, Any],
) -> MutableMapping[str, Any]:
    """Fold one phase outcome into the aggregated replay results.

    Records the phase result, appends its report's tables, publication
    receipts and errors without duplicates and recomputes ``rows`` as the
    sum of receipt rows.  A phase that ran, exited non-zero and reported
    no errors of its own contributes exactly one
    ``<phase>: runner exited <code>`` error, so a failed runner can never
    hide behind an empty report.
    """
    phases: MutableMapping[str, Any] = results["phases"]
    phases[phase] = dict(phase_result)
    report = phase_result.get("report")
    if not isinstance(report, Mapping):
        report = {}
    tables: list[Any] = results["tables"]
    receipts: list[Any] = results["publication_receipts"]
    errors: list[Any] = results["errors"]
    for table in report.get("tables") or []:
        if table not in tables:
            tables.append(table)
    for receipt in report.get("publication_receipts") or []:
        if receipt not in receipts:
            receipts.append(receipt)
    for error in phase_result.get("errors") or []:
        if error not in errors:
            errors.append(error)
    exit_code = phase_result.get("exit_code")
    if (
        phase_result.get("ran")
        and isinstance(exit_code, int)
        and not isinstance(exit_code, bool)
        and exit_code != 0
        and not (phase_result.get("errors") or [])
    ):
        runner_error = f"{phase}: runner exited {exit_code}"
        if runner_error not in errors:
            errors.append(runner_error)
    rows = 0
    for receipt in receipts:
        if isinstance(receipt, Mapping):
            try:
                rows += int(receipt.get("rows") or 0)
            except (TypeError, ValueError):
                continue
    results["rows"] = rows
    return results


def _selected_status_counts(
    capture_runtime: object, records: Sequence[EndpointManifest]
) -> dict[str, int]:
    """Classify the selected manifest keys once every phase has run.

    ``success``, ``legitimate_empty``, ``not_supported`` and
    ``schema_error`` mirror the committed manifest status of each selected
    record; ``deferred`` counts only replay work that remains — retryable
    failures, records the store cannot serve and unknown statuses.  A
    ``schema_error`` is never a success, and capped or unselected records
    do not appear in these counters at all.
    """
    counts = {
        "success": 0,
        "legitimate_empty": 0,
        "not_supported": 0,
        "schema_error": 0,
        "deferred": 0,
    }
    store = getattr(capture_runtime, "manifest_store", None)
    for record in records:
        status = None
        if store is not None:
            try:
                status = getattr(store.get(record.key), "status", None)
            except Exception:
                status = None
        if status == ManifestStatus.SUCCESS:
            counts["success"] += 1
        elif status == ManifestStatus.LEGITIMATE_EMPTY:
            counts["legitimate_empty"] += 1
        elif status == ManifestStatus.NOT_SUPPORTED:
            counts["not_supported"] += 1
        elif status == ManifestStatus.SCHEMA_ERROR:
            counts["schema_error"] += 1
        else:
            counts["deferred"] += 1
    return counts


def run_replay_scope(
    scope: object,
    *,
    mode: str,
    target_cap: int = 25,
    capture_runtime: object,
    records: Optional[Sequence[EndpointManifest]] = None,
) -> dict[str, Any]:
    """Run the season, match and player offline replay phases for one scope.

    Season finalize runs first and prepares the season manifest and player
    universe, the match phase then publishes the captured event evidence
    and the player phase runs last on top of that universe.  Plan
    selection inside :func:`_load_scope` runs with the caller's
    ``target_cap`` directly, without touching module globals.  Every phase
    merges through :func:`_merge_phase_report`, a phase error never stops
    the phases after it, the selected manifest keys are classified into
    success/legitimate_empty/not_supported/schema_error/deferred counters
    and traffic comes from a single final engine metrics snapshot.
    """
    scope_key = _normalize_scope(scope)
    resolved, plan = _load_scope(
        scope_key,
        mode=mode,
        capture_runtime=capture_runtime,
        records=records,
        target_cap=target_cap,
    )

    results: dict[str, Any] = {
        "entity": _REPLAY_ENTITY,
        "mode": mode,
        "scope": (
            scope_key.source_tournament_id,
            scope_key.source_season_id,
            scope_key.freshness_key,
        ),
        "league": getattr(resolved, "league", None),
        "canonical_season": getattr(resolved, "canonical_season", None),
        "selected": {"records": 0, "endpoints": {}, "targets": []},
        "skipped": 0,
        "phases": {},
        "tables": [],
        "publication_receipts": [],
        "rows": 0,
        "errors": [],
    }
    provided = len(records) if records is not None else 0
    selected_records: tuple[EndpointManifest, ...] = ()
    if plan is None:
        results["skipped"] = provided
        for phase in ("season", "matches", "players"):
            _merge_phase_report(
                results,
                phase,
                {
                    "ran": False,
                    "reason": "no_replayable_records",
                    "targets": [],
                    "errors": [],
                },
            )
    else:
        plan_records = tuple(plan.records)
        selected_records = plan_records
        results["skipped"] = max(0, provided - len(plan_records))
        targets: set[str] = set()
        for record in plan_records:
            key = record.key
            target_type = getattr(key, "target_type", None) or key.endpoint
            targets.add(f"{target_type}:{key.target_id}")
        results["selected"] = {
            "records": len(plan_records),
            "endpoints": dict(
                Counter(record.key.endpoint for record in plan_records)
            ),
            "targets": sorted(targets),
        }
        phases = (
            (
                "season",
                lambda: _season_phase(
                    resolved, plan_records, scope_key, capture_runtime
                ),
            ),
            (
                "matches",
                lambda: _matches_phase(
                    resolved, plan_records, capture_runtime, mode=mode
                ),
            ),
            (
                "players",
                lambda: _players_phase(
                    resolved, plan_records, capture_runtime, mode=mode
                ),
            ),
        )
        for phase, run_phase in phases:
            try:
                phase_result = run_phase()
            except Exception as exc:
                phase_result = {
                    "ran": False,
                    "reason": "phase_error",
                    "targets": [],
                    "report": {},
                    "errors": [f"{phase}: {exc}"],
                }
            _merge_phase_report(results, phase, phase_result)
    results["status_counts"] = _selected_status_counts(
        capture_runtime, selected_records
    )
    traffic = capture_runtime.engine.metrics.snapshot()
    results["traffic"] = traffic
    results["source_request_count"] = traffic.get("source_request_count", 0)
    return results


def _build_cli_parser(parser_class: type) -> argparse.ArgumentParser:
    """Build the strict #1367 offline replay CLI parser."""
    parser = parser_class(
        description="Offline SofaScore raw replay of one exact scope (#1367)."
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=tuple(MODES),
        help="Replay mode: 'fields' or 'drain'.",
    )
    parser.add_argument(
        "--scope-json",
        required=True,
        help=(
            "JSON object with exactly the three string fields "
            "(source_tournament_id, source_season_id, freshness_key)."
        ),
    )
    parser.add_argument(
        "--target-cap",
        type=int,
        default=25,
        help="Maximum replayed targets per plan; must be positive.",
    )
    parser.add_argument(
        "--output",
        default=_DEFAULT_REPLAY_OUTPUT,
        help="Output file for the replay result payload.",
    )
    parser.add_argument(
        "--raw-store-uri",
        default=None,
        help="Override SOFASCORE_RAW_STORE_URI for capture/replay.",
    )
    parser.add_argument(
        "--manifest-backend",
        choices=("trino", "json"),
        default="trino",
        help="Endpoint manifest backend for the replay runtime.",
    )
    parser.add_argument(
        "--manifest-path",
        default=None,
        help="JSON manifest file; only valid with --manifest-backend json.",
    )
    return parser


def _cli_error_payload(
    mode: Optional[str], errors: Sequence[str]
) -> dict[str, Any]:
    """The early-failure payload: errors plus a provably zero-traffic block."""
    payload: dict[str, Any] = {
        "entity": _REPLAY_ENTITY,
        "tables": [],
        "errors": list(errors),
        "traffic": {
            "paid_proxy_bytes": 0,
            "browser_sessions": 0,
            "browser_navigations": 0,
            "requests": 0,
            "source_request_count": 0,
        },
    }
    if mode is not None:
        payload["mode"] = mode
    return payload


def _parse_cli_scope(value: str) -> ReplayScopeKey:
    """Parse ``--scope-json`` into an exact replay scope key.

    The payload must be a JSON object with exactly
    ``source_tournament_id``/``source_season_id``/``freshness_key`` and a
    string value for each; anything else raises
    :class:`ReplayScopeError` before a capture runtime exists.
    """
    try:
        loaded = json.loads(value)
    except ValueError as exc:
        raise ReplayScopeError(
            f"--scope-json is not valid JSON: {exc}"
        ) from exc
    if not isinstance(loaded, dict):
        raise ReplayScopeError("--scope-json must be a JSON object")
    fields = set(loaded)
    expected = set(_SCOPE_FIELDS)
    if fields != expected:
        raise ReplayScopeError(
            "--scope-json must contain exactly (source_tournament_id, "
            "source_season_id, freshness_key); "
            f"missing={sorted(expected - fields)}, "
            f"extra={sorted(fields - expected)}"
        )
    values: dict[str, str] = {}
    for name in _SCOPE_FIELDS:
        item = loaded[name]
        if isinstance(item, bool) or not isinstance(item, str):
            raise ReplayScopeError(
                f"--scope-json field {name} must be a string"
            )
        values[name] = item
    return _normalize_scope(values)


def _json_manifest_path(explicit: Optional[str]) -> str:
    """Resolve the json manifest path like canonical ``build_manifest_store``.

    An explicit ``--manifest-path`` wins; otherwise the path comes from
    ``SOFASCORE_MANIFEST_PATH`` with the same
    ``/tmp/sofascore-endpoint-manifest.json`` default the canonical
    builder uses.
    """
    if explicit is not None:
        return explicit
    return os.environ.get(
        "SOFASCORE_MANIFEST_PATH",
        "/tmp/sofascore-endpoint-manifest.json",
    )


def _json_manifest_store(path: str) -> JsonFileManifestStore:
    """Build the canonical JSON file manifest store for offline replay."""
    return JsonFileManifestStore(path)


def _manifest_store_scope_records(
    store: object, scope: ReplayScopeKey
) -> Optional[Sequence[EndpointManifest]]:
    """Read one scope's records from a manifest store that can list them."""
    list_scope = getattr(store, "list_for_scope", None)
    if not callable(list_scope):
        return None
    return list_scope(scope.source_tournament_id, scope.source_season_id)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Strict #1367 CLI: replay one exact offline SofaScore scope.

    Invalid input fails with exit 1 and an errors payload before any capture
    runtime is built; ``--manifest-path`` is only valid with the json
    manifest backend.  For backend=json the store is always built from the
    explicit flag or from ``SOFASCORE_MANIFEST_PATH`` (with canonical
    ``build_manifest_store``'s default) and its records load through
    ``list_for_scope``, so no trino manager is ever required.  A replay
    that completes writes its results through
    ``run_sofascore_scraper._write_results`` and returns 0 only when the
    replay reported no errors.
    """
    from dags.scripts import run_sofascore_scraper as runner

    parser = _build_cli_parser(runner._StrictArgumentParser)
    try:
        args = parser.parse_args(argv)
    except runner._ArgparseError as exc:
        runner._write_results(
            _DEFAULT_REPLAY_OUTPUT,
            _cli_error_payload(None, [f"cli: {exc}"]),
        )
        return 1

    errors: list[str] = []
    scope_key: Optional[ReplayScopeKey] = None
    try:
        scope_key = _parse_cli_scope(args.scope_json)
    except ReplayScopeError as exc:
        errors.append(f"scope_json: {exc}")
    if args.target_cap < 1:
        errors.append(
            f"target_cap must be a positive integer, got {args.target_cap}"
        )
    if args.manifest_path is not None and args.manifest_backend != "json":
        errors.append(
            "--manifest-path is only valid with --manifest-backend json"
        )
    if errors:
        runner._write_results(
            args.output,
            _cli_error_payload(args.mode, errors),
        )
        return 1

    capture_runtime = None
    records: Optional[Sequence[EndpointManifest]] = None
    try:
        from scrapers.sofascore.pipeline import build_capture_runtime

        manifest_store = (
            _json_manifest_store(_json_manifest_path(args.manifest_path))
            if args.manifest_backend == "json"
            else None
        )
        capture_runtime = build_capture_runtime(
            run_id=(
                os.environ.get("SOFASCORE_RUN_ID")
                or os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
                or f"manual-{os.getpid()}"
            ),
            task_id=(
                os.environ.get("AIRFLOW_CTX_TASK_ID")
                or "cli-sofascore-raw-replay"
            ),
            raw_store_uri=args.raw_store_uri,
            manifest_backend=args.manifest_backend,
            **(
                {"manifest_store": manifest_store}
                if manifest_store is not None
                else {}
            ),
        )
        if manifest_store is not None:
            records = _manifest_store_scope_records(
                manifest_store, scope_key
            )
    except Exception as exc:
        runner._write_results(
            args.output,
            _cli_error_payload(args.mode, [f"capture_runtime: {exc}"]),
        )
        return 1

    try:
        results = run_replay_scope(
            scope_key,
            mode=args.mode,
            target_cap=args.target_cap,
            capture_runtime=capture_runtime,
            records=records,
        )
    except Exception as exc:
        results = _cli_error_payload(args.mode, [f"replay: {exc}"])
    runner._write_results(args.output, results)
    return 0 if not results.get("errors") else 1


if __name__ == "__main__":
    sys.exit(main())
