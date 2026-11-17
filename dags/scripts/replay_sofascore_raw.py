"""Offline SofaScore raw-replay worker core (#1367).

These helpers normalize one exact replay scope, resolve it through the
tournament catalog, select the planner's deterministic raw-replay plan and
rebuild exact season endpoint specs from already-captured manifest records —
all network-free.  Transport, phases, CLI and DAG wiring deliberately stay
out of this module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Sequence

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
