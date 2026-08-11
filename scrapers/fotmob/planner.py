"""Deterministic, budgeted planning for FotMob discovery and backfill."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Iterable, Mapping, Optional

from .domain import ScopeClassification, ScopeDecision, SeasonRef


MIB = 1024 * 1024

# Acceptance sentinels.  Qualification/play-off competitions are separate
# FotMob identities and must not be folded into their parent competition.
MANDATORY_COMPETITION_IDS = frozenset(
    {
        47,  # Premier League
        42,  # UEFA Champions League
        10611,  # UEFA Champions League qualification
        9806,  # Nations League A
        9807,  # Nations League B
        9808,  # Nations League C
        9809,  # Nations League D
        10557,  # Nations League A qualification (source-era identity)
        10558,  # Nations League B qualification (source-era identity)
        10717,  # Nations League A qualification
        10718,  # Nations League B qualification
        10719,  # Nations League C qualification
        63,  # Russian Premier League
        9333,  # Russian Premier League qualification
        289,  # Africa Cup of Nations
        10608,  # Africa Cup of Nations qualification
    }
)


class RunMode(str, Enum):
    DISCOVER = "discover"
    DAILY = "daily"
    BACKFILL = "backfill"
    REPLAY = "replay"


class ScopeLane(str, Enum):
    """Disjoint automatic queues for source-current and historical seasons."""

    CURRENT = "current"
    HISTORY = "history"


@dataclass(frozen=True, slots=True)
class ScopeAttemptState:
    """Latest durable scheduler result for one exact contract-bound scope."""

    competition_id: int
    source_season_key: str
    plan_signature: str
    attempt_count: int
    last_attempt_at: datetime
    next_retry_at: Optional[datetime]
    outcome: str
    reason: str
    attempt_identities: tuple[str, ...] = ()

    @property
    def identity(self) -> tuple[int, str]:
        return self.competition_id, self.source_season_key


SCOPE_PLAN_SIGNATURE_VERSION = "fotmob-scope-plan-v1"

# Сколько терминальный исход держит скоуп вне планов. Пока журнал жил под
# контентной подписью, карта состояний была пуста на каждом ране и вечное
# исключение никого не задевало. Со стабильной подписью одна разовая ошибка
# коммита вычеркнула бы скоуп из обхода навсегда — тихая дыра в покрытии,
# которую не видно ни по цвету рана, ни по числу закрытых скоупов.
TERMINAL_RETRY_AFTER = timedelta(hours=24)


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def deterministic_plan_signature(
    entities: Iterable[str],
    policy: Optional[Mapping[str, Any]] = None,
    *,
    version: str = SCOPE_PLAN_SIGNATURE_VERSION,
) -> str:
    """Return an order-independent signature for one coverage obligation.

    A backfill scope is complete only for the exact set of requested entities
    and behavior-affecting policy.  Runtime knobs such as worker count do not
    belong in ``policy``; completeness knobs such as ``include_unfinished``
    and transfer recency do.  The explicit signature version makes future
    policy-normalization changes fail closed instead of reusing old markers.
    """

    normalized_entities = sorted(
        {str(entity).strip().lower() for entity in entities if str(entity).strip()}
    )
    if not normalized_entities:
        raise ValueError("at least one FotMob entity is required")
    if not str(version).strip():
        raise ValueError("plan signature version must not be empty")
    material = json.dumps(
        {
            "entities": normalized_entities,
            "policy": dict(policy or {}),
            "version": str(version),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "fmplan1-" + hashlib.sha256(material).hexdigest()


class BudgetExceeded(RuntimeError):
    """A run attempted to exceed its explicit request/byte ceiling."""


@dataclass(frozen=True)
class TransportBudget:
    max_requests: int = 2_000
    max_direct_bytes: int = 256 * MIB
    max_proxy_bytes: int = 0

    def __post_init__(self) -> None:
        if self.max_requests < 0:
            raise ValueError("max_requests must be non-negative")
        if self.max_direct_bytes < 0 or self.max_proxy_bytes < 0:
            raise ValueError("byte budgets must be non-negative")


@dataclass
class BudgetLedger:
    budget: TransportBudget
    requests: int = 0
    direct_bytes: int = 0
    proxy_bytes: int = 0

    def reserve_request(self) -> None:
        if self.requests >= self.budget.max_requests:
            raise BudgetExceeded(
                f"FotMob request budget exhausted ({self.requests}/"
                f"{self.budget.max_requests})"
            )
        self.requests += 1

    def account_fetch(
        self,
        *,
        attempts: int,
        direct_bytes: int,
        proxy_bytes: int,
    ) -> None:
        """Account a completed transport call, including internal retries."""

        attempts = max(0, int(attempts))
        next_requests = self.requests + attempts
        if next_requests > self.budget.max_requests:
            raise BudgetExceeded(
                "FotMob request budget exceeded by transport retries: "
                f"{next_requests}>{self.budget.max_requests}"
            )
        self.account_bytes(
            direct_bytes=direct_bytes,
            proxy_bytes=proxy_bytes,
        )
        self.requests = next_requests

    def account_bytes(self, *, direct_bytes: int, proxy_bytes: int) -> None:
        next_direct = self.direct_bytes + max(0, int(direct_bytes))
        next_proxy = self.proxy_bytes + max(0, int(proxy_bytes))
        if next_direct > self.budget.max_direct_bytes:
            raise BudgetExceeded(
                "FotMob direct-byte budget exceeded: "
                f"{next_direct}>{self.budget.max_direct_bytes}"
            )
        if next_proxy > self.budget.max_proxy_bytes:
            raise BudgetExceeded(
                "FotMob proxy-byte invariant exceeded: "
                f"{next_proxy}>{self.budget.max_proxy_bytes}"
            )
        self.direct_bytes = next_direct
        self.proxy_bytes = next_proxy

    @property
    def remaining_requests(self) -> int:
        return self.budget.max_requests - self.requests

    def as_dict(self) -> dict[str, int]:
        return {
            "requests": self.requests,
            "direct_bytes": self.direct_bytes,
            "proxy_bytes": self.proxy_bytes,
            "max_requests": self.budget.max_requests,
            "max_direct_bytes": self.budget.max_direct_bytes,
            "max_proxy_bytes": self.budget.max_proxy_bytes,
        }


@dataclass(frozen=True)
class SeasonWorkItem:
    competition_id: int
    source_season_key: str
    priority: tuple[Any, ...]
    is_latest: bool
    reason: str

    @property
    def identity(self) -> tuple[int, str]:
        return self.competition_id, self.source_season_key


def _season_recency_key(season: SeasonRef) -> int:
    """Use source order only; never infer/normalize the season identity."""

    if season.source_order is not None:
        return int(season.source_order)
    return 0


def plan_seasons(
    classifications: Iterable[ScopeClassification],
    seasons: Iterable[SeasonRef],
    *,
    mode: RunMode,
    previously_successful: Iterable[tuple[int, str]] = (),
    explicit_scopes: Optional[Iterable[tuple[int, str]]] = None,
    lane: Optional[ScopeLane] = None,
    attempt_states: Mapping[tuple[int, str], ScopeAttemptState] | None = None,
    now: Optional[datetime] = None,
) -> list[SeasonWorkItem]:
    """Build a stable full-catalog/daily/backfill plan.

    Included adult men's competitions are eligible.  Excluded and ambiguous
    competitions remain in the discovery catalog but never enter an ingest
    plan. Current and history lanes are disjoint. A durable retry that is not
    due is skipped without blocking later ready work. Exact season strings are
    passed through unchanged and no hardcoded competition cohort affects order.
    """

    included = {
        item.competition.competition_id
        for item in classifications
        if item.decision == ScopeDecision.INCLUDED
    }
    success = {(int(comp), str(season)) for comp, season in previously_successful}
    requested = (
        {(int(comp), str(season)) for comp, season in explicit_scopes}
        if explicit_scopes is not None
        else None
    )
    attempts = dict(attempt_states or {})
    observed_now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    if observed_now.tzinfo is not None:
        observed_now = observed_now.astimezone(timezone.utc).replace(tzinfo=None)

    output: list[SeasonWorkItem] = []
    seen: set[tuple[int, str]] = set()
    for season in seasons:
        identity = season.identity
        if identity in seen:
            continue
        seen.add(identity)
        if season.competition_id not in included:
            continue
        if requested is not None and identity not in requested:
            continue
        is_current = bool(season.is_selected or season.is_latest)
        if lane == ScopeLane.CURRENT and not is_current:
            continue
        if lane == ScopeLane.HISTORY and is_current:
            continue
        if mode == RunMode.DAILY and not (season.is_selected or season.is_latest):
            continue
        if mode == RunMode.BACKFILL and identity in success:
            continue
        if mode == RunMode.REPLAY and identity not in success:
            # Replay is bounded to known raw identities, not a network backfill.
            continue

        attempt = attempts.get(identity)
        if attempt is not None:
            if (
                attempt.outcome in {"success", "source_gap"}
                and lane != ScopeLane.CURRENT
            ):
                continue
            if (
                attempt.outcome == "terminal"
                and observed_now - _naive_utc(attempt.last_attempt_at)
                < TERMINAL_RETRY_AFTER
            ):
                continue
            retry_at = attempt.next_retry_at
            if retry_at is not None:
                if retry_at.tzinfo is not None:
                    retry_at = retry_at.astimezone(timezone.utc).replace(tzinfo=None)
                if retry_at > observed_now:
                    continue

        active_rank = 0 if (season.is_selected or season.is_latest) else 1
        recency = _season_recency_key(season)
        reason = "active_or_latest" if active_rank == 0 else "historical_backfill"
        last_attempt = attempt.last_attempt_at if attempt is not None else datetime.min
        if last_attempt.tzinfo is not None:
            last_attempt = last_attempt.astimezone(timezone.utc).replace(tzinfo=None)
        output.append(
            SeasonWorkItem(
                competition_id=season.competition_id,
                source_season_key=season.source_season_key,
                priority=(
                    active_rank,
                    last_attempt,
                    recency,
                    season.source_season_key,
                ),
                is_latest=bool(season.is_selected or season.is_latest),
                reason=reason,
            )
        )
    return sorted(
        output,
        key=lambda item: (item.priority, item.competition_id),
    )


def tombstones_after_two_absences(
    previous_snapshot_ids: Iterable[int],
    snapshot_before_previous_ids: Iterable[int],
    current_snapshot_ids: Iterable[int],
) -> set[int]:
    """Identify ids absent from two consecutive *complete* snapshots.

    The older snapshot establishes that an id existed.  It is tombstoned only
    when missing from both the previous and current complete discoveries.
    """

    older = {int(item) for item in snapshot_before_previous_ids}
    previous = {int(item) for item in previous_snapshot_ids}
    current = {int(item) for item in current_snapshot_ids}
    return older - previous - current


def utc_run_id(prefix: str = "fotmob") -> str:
    now = datetime.now(timezone.utc)
    return f"{prefix}-{now:%Y%m%dT%H%M%S.%fZ}"


__all__ = [
    "MANDATORY_COMPETITION_IDS",
    "SCOPE_PLAN_SIGNATURE_VERSION",
    "BudgetExceeded",
    "BudgetLedger",
    "RunMode",
    "ScopeAttemptState",
    "ScopeLane",
    "SeasonWorkItem",
    "TransportBudget",
    "deterministic_plan_signature",
    "plan_seasons",
    "tombstones_after_two_absences",
    "utc_run_id",
]
