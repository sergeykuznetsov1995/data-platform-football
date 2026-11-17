"""Pure selection of SofaScore raw-replay scope plans (#1367).

This module deliberately contains no SQL, catalog or worker wiring: it only
turns already-captured endpoint manifest records into deterministic replay
plans for committed raw blobs.

A record is *replayable* when its raw payload is readable: ``http_status`` is
a 2xx other than 204 and both ``raw_content_hash`` and ``raw_blob_key`` are
non-empty.  Everything else is excluded from replay work.

Two modes exist:

``fields``
    Re-parse paid field endpoints (``lineups``, ``standings_total``,
    ``rounds``, ``cup_trees``, ``participants``, ``referee_profile``) with any
    manifest status, so an old ``success`` capture is replayed after a parser
    upgrade.

``drain``
    Re-parse deferred work: any endpoint whose last observation ended as
    ``retryable_failure`` while the raw payload itself is replayable
    (generic HTTP 200 deferred records).

Selection is deterministic: scopes are ordered by
``(source_tournament_id, source_season_id, freshness_key)``, targets inside a
scope by ``(target_type, target_id)`` and records inside a target by
endpoint.  Caps drop whole scopes or whole targets, never a partial endpoint
set of one target.
"""

from __future__ import annotations

from typing import Iterable, NamedTuple, Optional, Sequence

from .manifest import EndpointManifest, ManifestStatus

__all__ = [
    "FIELDS_ENDPOINTS",
    "MODES",
    "MODE_DRAIN",
    "MODE_FIELDS",
    "ReplayPlanningError",
    "ReplayScopeKey",
    "ReplayScopePlan",
    "ReplaySelection",
    "select_replay_scope_plans",
]

MODE_FIELDS = "fields"
MODE_DRAIN = "drain"
MODES = (MODE_FIELDS, MODE_DRAIN)

FIELDS_ENDPOINTS = frozenset(
    {
        "lineups",
        "standings_total",
        "rounds",
        "cup_trees",
        "participants",
        "referee_profile",
    }
)

SKIP_NON_2XX = "non_2xx"
SKIP_MISSING_LINEAGE = "missing_lineage"
SKIP_OTHER_STATUS = "other_status"
SKIP_REASONS = (SKIP_NON_2XX, SKIP_MISSING_LINEAGE, SKIP_OTHER_STATUS)


class ReplayPlanningError(ValueError):
    """Invalid replay selection arguments (mode, caps or exact_scope)."""


ReplayScopeKey = NamedTuple(
    "ReplayScopeKey",
    source_tournament_id=str,
    source_season_id=str,
    freshness_key=str,
)

ReplayScopePlan = NamedTuple(
    "ReplayScopePlan",
    scope=ReplayScopeKey,
    records=tuple[EndpointManifest, ...],
    mode=str,
)


class ReplaySelection(tuple):
    """Tuple of :class:`ReplayScopePlan` plus pre-cap skip diagnostics.

    ``skipped`` counts records excluded by the replayability/status filters
    before caps are applied.  Records dropped by ``scope_cap``/``target_cap``
    or narrowed away by ``exact_scope`` and the mode's endpoint universe are
    intentionally not counted.
    """

    def __new__(cls, plans, skipped) -> "ReplaySelection":
        selection = super().__new__(cls, plans)
        counts = {reason: 0 for reason in SKIP_REASONS}
        for reason, count in dict(skipped).items():
            if reason in counts:
                counts[reason] = int(count)
        selection._skipped = counts
        return selection

    @property
    def skipped(self) -> dict[str, int]:
        return dict(self._skipped)


def _validate_positive_int(value: object, name: str) -> int:
    if not isinstance(value, int) or value <= 0:
        raise ReplayPlanningError(
            f"{name} must be a positive integer, got {value!r}"
        )
    return value


def _normalize_exact_scope(exact_scope: object) -> Optional[tuple[str, str]]:
    if exact_scope is None:
        return None
    if isinstance(exact_scope, (str, bytes)) or not isinstance(
        exact_scope, Sequence
    ):
        raise ReplayPlanningError(
            "exact_scope must be a (source_tournament_id, source_season_id) pair"
        )
    pair = tuple(exact_scope)
    if len(pair) != 2:
        raise ReplayPlanningError(
            "exact_scope must contain exactly "
            "(source_tournament_id, source_season_id), "
            f"got {len(pair)} values"
        )
    normalized = []
    for value in pair:
        if not isinstance(value, (str, int)):
            raise ReplayPlanningError(
                f"exact_scope ids must be str or int, got {value!r}"
            )
        text = str(value).strip()
        if not text:
            raise ReplayPlanningError("exact_scope ids must not be empty")
        normalized.append(text)
    return (normalized[0], normalized[1])


def _is_replayable_http(record: EndpointManifest) -> bool:
    status = record.http_status
    return status is not None and 200 <= status < 300 and status != 204


def _has_raw_lineage(record: EndpointManifest) -> bool:
    return bool((record.raw_content_hash or "").strip()) and bool(
        (record.raw_blob_key or "").strip()
    )


def _skip_reason(record: EndpointManifest, mode: str) -> Optional[str]:
    if not _is_replayable_http(record):
        return SKIP_NON_2XX
    if not _has_raw_lineage(record):
        return SKIP_MISSING_LINEAGE
    if mode == MODE_DRAIN and record.status != ManifestStatus.RETRYABLE_FAILURE:
        return SKIP_OTHER_STATUS
    return None


def _record_sort_key(record: EndpointManifest) -> tuple[str, str, str, str]:
    return (record.key.endpoint, record.run_id, record.task_id, record.updated_at)


def select_replay_scope_plans(
    records: Iterable[EndpointManifest],
    *,
    mode: str,
    scope_cap: int = 1,
    target_cap: int = 25,
    exact_scope: Optional[Sequence[object]] = None,
) -> tuple[ReplayScopePlan, ...]:
    """Select deterministic raw-replay plans from manifest records.

    mode ``fields`` replays paid field endpoints with any status (including
    old ``success``); mode ``drain`` replays any endpoint that ended as
    ``retryable_failure`` with a replayable 2xx raw payload.  ``exact_scope``
    filters records to one ``(source_tournament_id, source_season_id)``
    before grouping.  ``scope_cap`` limits the number of scopes and
    ``target_cap`` the number of ``(target_type, target_id)`` groups per
    scope; targets are always selected whole, so a cap never splits a
    target's endpoint set.

    The returned tuple subclass exposes a ``skipped`` property counting the
    records excluded before caps, by reason (``non_2xx``,
    ``missing_lineage``, ``other_status``).

    Raises :class:`ReplayPlanningError` for an unknown mode, non-positive or
    non-integer caps, or a malformed ``exact_scope``.
    """
    if mode not in MODES:
        raise ReplayPlanningError(f"mode must be one of {MODES}, got {mode!r}")
    scope_cap = _validate_positive_int(scope_cap, "scope_cap")
    target_cap = _validate_positive_int(target_cap, "target_cap")
    wanted_scope = _normalize_exact_scope(exact_scope)

    skipped = {reason: 0 for reason in SKIP_REASONS}
    by_scope: dict[tuple[str, str, str], list[EndpointManifest]] = {}

    for record in records:
        key = record.key
        if wanted_scope is not None and (
            key.source_tournament_id,
            key.source_season_id,
        ) != wanted_scope:
            continue
        if mode == MODE_FIELDS and key.endpoint not in FIELDS_ENDPOINTS:
            continue
        reason = _skip_reason(record, mode)
        if reason is not None:
            skipped[reason] += 1
            continue
        by_scope.setdefault(
            (key.source_tournament_id, key.source_season_id, key.freshness_key),
            [],
        ).append(record)

    plans: list[ReplayScopePlan] = []
    for scope_values in sorted(by_scope)[:scope_cap]:
        by_target: dict[tuple[str, str], list[EndpointManifest]] = {}
        for record in by_scope[scope_values]:
            by_target.setdefault(
                (record.key.target_type, record.key.target_id), []
            ).append(record)
        selected: list[EndpointManifest] = []
        for target in sorted(by_target)[:target_cap]:
            selected.extend(sorted(by_target[target], key=_record_sort_key))
        plans.append(
            ReplayScopePlan(
                scope=ReplayScopeKey(*scope_values),
                records=tuple(selected),
                mode=mode,
            )
        )
    return ReplaySelection(plans, skipped)
