"""Offline SofaScore raw-replay worker core (#1367).

These helpers normalize one exact replay scope, resolve it through the
tournament catalog, select the planner's deterministic raw-replay plan and
rebuild exact season endpoint specs from already-captured manifest records —
all network-free.  Transport, phases, CLI and DAG wiring deliberately stay
out of this module.
"""

from __future__ import annotations

import json
import os
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
from scrapers.sofascore.manifest import EndpointManifest, ManifestKey
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

_REPLAY_HISTORY_IDS_ENV = "SOFASCORE_HISTORY_MATCH_IDS_JSON"
_REPLAY_PHASE_ENV = "SOFASCORE_HISTORY_PHASE"
_REPLAY_EVIDENCE_ENV = "SOFASCORE_HISTORY_SEASON_EVIDENCE"
_REPLAY_PLAYER_IDS_ENV = "SOFASCORE_OFFLINE_PLAYER_IDS_JSON"
_REPLAY_PLAYER_FRESHNESS_ENV = "SOFASCORE_PLAYER_FRESHNESS_KEY"


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
) -> tuple[ResolvedReplayScope, Optional[ReplayScopePlan]]:
    """Resolve one exact replay scope and select its raw-replay plan.

    The catalog comes from the runtime when it carries one and otherwise from
    ``SofaScoreCatalog.load``.  Records already in memory are prefiltered to
    the exact scope triple; when absent they are loaded through the manifest
    store's trino manager with the planner's mode filters.  Returns the
    catalog-resolved scope and the single selected plan, or ``None`` when
    nothing is replayable.
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
        target_cap=25,
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

    Fields mode replays ``lineups`` targets, drain mode ``event`` targets.
    The phase refuses to run without targets or when the scope freshness
    key is not ``final``.  The runner writes its JSON report inside a
    temporary directory and the report is read back defensively.
    """
    if mode is None:
        mode = "fields" if _target_ids(records, "lineups") else "drain"
    ids = _target_ids(records, "lineups" if mode == "fields" else "event")
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
    records, publishes them plus the scope freshness key through env
    overrides and reads the runner's JSON report back defensively.
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


def _merge_phase_report(
    results: MutableMapping[str, Any],
    phase: str,
    phase_result: Mapping[str, Any],
) -> MutableMapping[str, Any]:
    """Fold one phase outcome into the aggregated replay results.

    Records the phase result, appends its report's tables, publication
    receipts and errors without duplicates and recomputes ``rows`` as the
    sum of receipt rows.
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
    rows = 0
    for receipt in receipts:
        if isinstance(receipt, Mapping):
            try:
                rows += int(receipt.get("rows") or 0)
            except (TypeError, ValueError):
                continue
    results["rows"] = rows
    return results


def run_replay_scope(
    scope: object,
    *,
    mode: str,
    target_cap: int = 25,
    capture_runtime: object,
    records: Optional[Sequence[EndpointManifest]] = None,
) -> dict[str, Any]:
    """Run the match and player offline replay phases for one exact scope.

    The plan selection inside :func:`_load_scope` temporarily runs with the
    caller's ``target_cap`` instead of its built-in default.  Both phases
    merge through :func:`_merge_phase_report` and traffic comes from a
    single final engine metrics snapshot.
    """
    scope_key = _normalize_scope(scope)
    original_select = select_replay_scope_plans

    def _select_with_cap(
        candidate_records: object, **kwargs: object
    ) -> object:
        kwargs["target_cap"] = target_cap
        return original_select(candidate_records, **kwargs)

    module_globals = globals()
    module_globals["select_replay_scope_plans"] = _select_with_cap
    try:
        resolved, plan = _load_scope(
            scope_key,
            mode=mode,
            capture_runtime=capture_runtime,
            records=records,
        )
    finally:
        module_globals["select_replay_scope_plans"] = original_select

    results: dict[str, Any] = {
        "entity": "sofascore_raw_replay",
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
    if plan is None:
        results["skipped"] = provided
        for phase in ("matches", "players"):
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
        _merge_phase_report(
            results,
            "matches",
            _matches_phase(resolved, plan_records, capture_runtime, mode=mode),
        )
        _merge_phase_report(
            results,
            "players",
            _players_phase(resolved, plan_records, capture_runtime, mode=mode),
        )
    traffic = capture_runtime.engine.metrics.snapshot()
    results["traffic"] = traffic
    results["source_request_count"] = traffic["source_request_count"]
    return results
