"""Typed values exchanged between FBref workers and the control store."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class CompetitionRegistryEntry:
    competition_id: str
    canonical_url: str
    name: str
    gender: str
    classification: str
    calendar_type: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SeasonRegistryEntry:
    competition_id: str
    season_id: str
    canonical_url: str
    label: Optional[str] = None
    is_current: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FrontierTarget:
    target_id: str
    page_kind: str
    canonical_url: str
    source_ids: Mapping[str, str]
    refresh_policy: str
    priority: int = 0
    next_fetch_at: Optional[datetime] = None
    source: str = "fbref"


@dataclass(frozen=True)
class FrontierProvenance:
    """Immutable evidence that one parsed page discovered another target."""

    parent_target_id: str
    child_target_id: str
    relation: str
    parent_content_hash: str
    parser_version: str
    carried_competition_id: Optional[str] = None
    carried_season_id: Optional[str] = None
    logical_refresh_id: Optional[str] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SeasonAlias:
    """A source/legacy season token mapped to one canonical season row."""

    competition_id: str
    alias: str
    season_id: str
    alias_kind: str = "source"
    source: str = "fbref"
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CohortTarget:
    target_id: str
    logical_refresh_id: str
    ordinal: int


@dataclass(frozen=True)
class TargetLease:
    attempt_id: str
    run_id: str
    target_id: str
    logical_refresh_id: str
    canonical_url: str
    page_kind: str
    source_ids: Mapping[str, str]
    claim_token: str
    lease_epoch: int
    attempt_number: int
    leased_by: str
    lease_expires_at: datetime


@dataclass(frozen=True)
class BudgetReservation:
    reservation_id: str
    run_id: str
    logical_refresh_id: str
    requests_reserved: int
    bytes_reserved: int
    status: str
    requests_used: Optional[int] = None
    bytes_used: Optional[int] = None


@dataclass(frozen=True)
class ThrottleSlot:
    domain: str
    slot_token: str
    lease_epoch: int
    scheduled_at: datetime


@dataclass(frozen=True)
class ObservationLease:
    """Exclusive parser claim for one immutable raw observation."""

    logical_refresh_id: str
    target_id: str
    content_hash: str
    parser_version: str
    typed_parser_version: str
    stateful_parser_version: str
    claim_token: str
    lease_expires_at: datetime
