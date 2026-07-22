"""Pure durable-control contract for Transfermarkt historical backfill.

The module owns no connections and performs no I/O.  It defines the exact
versioned records a DAG may persist, deterministic state transitions, and SQL
builders whose execution remains the responsibility of an approved task.

Two properties are intentional:

* campaign and batch identities contain only frozen semantic inputs, never
  wall-clock or mutable status fields;
* every transition is fail-closed and returns a new immutable record suitable
  for a compare-and-swap MERGE.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, fields, is_dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from typing import Any, Iterable, Mapping, Sequence

from scrapers.transfermarkt.models import (
    MAX_SCOPE_BATCH,
    SCOPE_WALL_CLOCK_TIMEOUT_SECONDS,
)


CONTRACT_VERSION = 1
REPORT_SCHEMA_VERSION = 1

CAMPAIGN_TABLE = "iceberg.ops.transfermarkt_backfill_campaign_v1"
SCOPE_TABLE = "iceberg.ops.transfermarkt_backfill_scope_v1"
ATTEMPT_TABLE = "iceberg.ops.transfermarkt_backfill_attempt_v1"
BATCH_TABLE = "iceberg.ops.transfermarkt_backfill_batch_v1"

LEASE_GRACE = timedelta(minutes=15)
STALE_LEASE_AFTER = timedelta(
    seconds=SCOPE_WALL_CLOCK_TIMEOUT_SECONDS
) + LEASE_GRACE

FIRST_SOURCE_RETRY_DELAY = timedelta(hours=1)
SECOND_SOURCE_RETRY_DELAY = timedelta(hours=24)
MAX_SOURCE_ATTEMPTS = 3
# Backward-compatible name for callers that imported the original v1 draft.
# Terminalisation is governed by ``MAX_SOURCE_ATTEMPTS``, not error count.
MAX_SOURCE_ERRORS = MAX_SOURCE_ATTEMPTS
UNAVAILABLE_CONFIRMATIONS_REQUIRED = 2

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class BackfillStateError(ValueError):
    """A control record or requested transition violates the contract."""


class CampaignStatus(str, Enum):
    WAITING_PREREQUISITE = "waiting_prerequisite"
    ACTIVE = "active"
    BLOCKED_PLATFORM = "blocked_platform"
    COMPLETE = "complete"


class ScopeStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    CAPTURED_PENDING_DQ = "captured_pending_dq"
    RETRYABLE_ERROR = "retryable_error"
    COMPLETE = "complete"
    UNAVAILABLE = "unavailable"
    TERMINAL_ERROR = "terminal_error"


class AttemptOutcome(str, Enum):
    CAPTURED = "captured"
    UNAVAILABLE_CONFIRMATION = "unavailable_confirmation"
    SOURCE_ERROR = "source_error"
    PLATFORM_ERROR = "platform_error"


class BatchStatus(str, Enum):
    CLAIMED = "claimed"
    RUNNING = "running"
    DQ_PENDING = "dq_pending"
    COMPLETE = "complete"
    BLOCKED_PLATFORM = "blocked_platform"


TERMINAL_SCOPE_STATUSES = frozenset(
    {
        ScopeStatus.COMPLETE,
        ScopeStatus.UNAVAILABLE,
        ScopeStatus.TERMINAL_ERROR,
    }
)


def _required_text(name: str, value: Any) -> str:
    if not isinstance(value, str):
        raise BackfillStateError(f"{name} must be a string")
    result = value.strip()
    if not result:
        raise BackfillStateError(f"{name} must not be empty")
    return result


def _required_sha256(name: str, value: Any) -> str:
    result = _required_text(name, value)
    if _SHA256.fullmatch(result) is None:
        raise BackfillStateError(f"{name} must be a lowercase sha256")
    return result


def _optional_artifact(
    uri_name: str,
    uri: str | None,
    digest_name: str,
    digest: str | None,
) -> tuple[str | None, str | None]:
    if (uri is None) != (digest is None):
        raise BackfillStateError(
            f"{uri_name} and {digest_name} must be set together"
        )
    if uri is None:
        return None, None
    return _required_text(uri_name, uri), _required_sha256(digest_name, digest)


def _utc_datetime(name: str, value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise BackfillStateError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise BackfillStateError(f"{name} must include a timezone")
    return value.astimezone(timezone.utc)


def _optional_utc_datetime(
    name: str, value: datetime | None
) -> datetime | None:
    if value is None:
        return None
    return _utc_datetime(name, value)


def _non_negative_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BackfillStateError(f"{name} must be a non-negative integer")
    return value


def _revision(value: Any) -> int:
    return _non_negative_int("revision", value)


def _normalise_json(value: Any) -> Any:
    """Convert contract values to one unambiguous JSON data model."""

    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: _normalise_json(getattr(value, item.name))
            for item in fields(value)
        }
    if isinstance(value, Enum):
        return _normalise_json(value.value)
    if isinstance(value, datetime):
        parsed = _utc_datetime("canonical datetime", value)
        return parsed.isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise BackfillStateError("canonical JSON object keys must be strings")
            result[key] = _normalise_json(item)
        return result
    if isinstance(value, (tuple, list)):
        return [_normalise_json(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise BackfillStateError(
        f"unsupported canonical JSON value: {type(value).__name__}"
    )


def canonical_json(value: Any) -> str:
    """Return UTF-8-safe canonical JSON used by every content address."""

    return json.dumps(
        _normalise_json(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _normalise_raw_evidence(values: Iterable[str]) -> tuple[str, ...]:
    result = tuple(sorted({_required_sha256("raw_evidence_id", item) for item in values}))
    return result


@dataclass(frozen=True)
class HistoricalScopeTarget:
    """One immutable, promoted, active senior-men historical scope."""

    scope_id: str
    competition_id: str
    edition_id: str
    canonical_competition_id: str
    canonical_season: str
    registry_snapshot_id: str
    current: bool = False
    gender: str = "men"
    age_category: str = "senior"
    active: bool = True
    classification_status: str = "eligible"
    contract_version: int = CONTRACT_VERSION

    def __post_init__(self) -> None:
        for name in (
            "scope_id",
            "competition_id",
            "edition_id",
            "canonical_competition_id",
            "canonical_season",
            "registry_snapshot_id",
        ):
            object.__setattr__(self, name, _required_text(name, getattr(self, name)))
        for name in ("gender", "age_category", "classification_status"):
            value = _required_text(name, getattr(self, name)).lower()
            object.__setattr__(self, name, value)
        if self.contract_version != CONTRACT_VERSION:
            raise BackfillStateError("unsupported historical-target contract version")
        if not isinstance(self.current, bool) or self.current:
            raise BackfillStateError("backfill target must be historical (current=false)")
        if not isinstance(self.active, bool) or not self.active:
            raise BackfillStateError("backfill target must be active in its frozen snapshot")
        if self.gender != "men" or self.age_category != "senior":
            raise BackfillStateError("backfill target must be senior men")
        if self.classification_status != "eligible":
            raise BackfillStateError("backfill target classification must be eligible")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "HistoricalScopeTarget":
        if not isinstance(value, Mapping):
            raise BackfillStateError("historical target must be an object")
        return cls(
            scope_id=value.get("scope_id", ""),
            competition_id=value.get("competition_id", ""),
            edition_id=value.get("edition_id", ""),
            canonical_competition_id=value.get("canonical_competition_id", ""),
            canonical_season=value.get("canonical_season", ""),
            registry_snapshot_id=value.get("registry_snapshot_id", ""),
            current=value.get("current", value.get("is_current", False)),
            gender=value.get("gender", ""),
            age_category=value.get("age_category", ""),
            active=value.get("active", True),
            classification_status=value.get("classification_status", "eligible"),
            contract_version=value.get("contract_version", CONTRACT_VERSION),
        )

    def identity_payload(self) -> dict[str, Any]:
        return _normalise_json(self)


def freeze_historical_targets(
    values: Iterable[HistoricalScopeTarget | Mapping[str, Any]],
    *,
    registry_snapshot_id: str | None = None,
) -> tuple[HistoricalScopeTarget, ...]:
    """Validate and canonically order the exact campaign denominator."""

    targets = tuple(
        item
        if isinstance(item, HistoricalScopeTarget)
        else HistoricalScopeTarget.from_mapping(item)
        for item in values
    )
    if not targets:
        raise BackfillStateError("historical target must not be empty")
    ordered = tuple(sorted(targets, key=lambda item: item.scope_id))
    scope_ids = [item.scope_id for item in ordered]
    if len(scope_ids) != len(set(scope_ids)):
        raise BackfillStateError("historical target contains duplicate scope_id")
    identities = {(item.competition_id, item.edition_id) for item in ordered}
    if len(identities) != len(ordered):
        raise BackfillStateError(
            "historical target contains duplicate competition/edition"
        )
    if registry_snapshot_id is not None:
        snapshot = _required_text("registry_snapshot_id", registry_snapshot_id)
        mismatched = [
            item.scope_id for item in ordered
            if item.registry_snapshot_id != snapshot
        ]
        if mismatched:
            raise BackfillStateError(
                "historical targets do not belong to the frozen registry snapshot: "
                + ", ".join(mismatched)
            )
    return ordered


def stable_campaign_id(
    *,
    registry_snapshot_id: str,
    policy_sha256: str,
    parser_revision: str,
    schema_revision: str,
    targets: Iterable[HistoricalScopeTarget | Mapping[str, Any]],
) -> str:
    snapshot = _required_text("registry_snapshot_id", registry_snapshot_id)
    frozen = freeze_historical_targets(targets, registry_snapshot_id=snapshot)
    return canonical_sha256(
        {
            "contract_version": CONTRACT_VERSION,
            "registry_snapshot_id": snapshot,
            "policy_sha256": _required_sha256("policy_sha256", policy_sha256),
            "parser_revision": _required_text("parser_revision", parser_revision),
            "schema_revision": _required_text("schema_revision", schema_revision),
            "targets": [item.identity_payload() for item in frozen],
        }
    )


def stable_batch_id(
    campaign_id: str,
    scope_ids: Sequence[str],
    scope_claim_generations: Sequence[int] | None = None,
) -> str:
    campaign = _required_sha256("campaign_id", campaign_id)
    if isinstance(scope_ids, (str, bytes)) or not scope_ids:
        raise BackfillStateError("batch scope_ids must be a non-empty sequence")
    scopes = tuple(_required_text("scope_id", item) for item in scope_ids)
    if len(scopes) > MAX_SCOPE_BATCH:
        raise BackfillStateError(
            f"batch cannot exceed MAX_SCOPE_BATCH={MAX_SCOPE_BATCH}"
        )
    if len(scopes) != len(set(scopes)):
        raise BackfillStateError("batch contains duplicate scope_id")
    if scope_claim_generations is None:
        generations = tuple(1 for _item in scopes)
    else:
        generations = tuple(
            _non_negative_int("scope claim generation", item)
            for item in scope_claim_generations
        )
        if len(generations) != len(scopes):
            raise BackfillStateError(
                "scope claim generations must align with scope_ids"
            )
        if any(item < 1 for item in generations):
            raise BackfillStateError("scope claim generation starts at one")
    return canonical_sha256(
        {
            "contract_version": CONTRACT_VERSION,
            "campaign_id": campaign,
            "ordered_scopes": [
                {"scope_id": scope_id, "claim_generation": generation}
                for scope_id, generation in zip(scopes, generations, strict=True)
            ],
        }
    )


@dataclass(frozen=True)
class BackfillCampaign:
    campaign_id: str
    registry_snapshot_id: str
    policy_sha256: str
    parser_revision: str
    schema_revision: str
    targets: tuple[HistoricalScopeTarget, ...]
    target_sha256: str
    status: CampaignStatus
    created_at: datetime
    updated_at: datetime
    report_uri: str | None = None
    report_sha256: str | None = None
    revision: int = 0
    contract_version: int = CONTRACT_VERSION

    def __post_init__(self) -> None:
        snapshot = _required_text("registry_snapshot_id", self.registry_snapshot_id)
        object.__setattr__(self, "registry_snapshot_id", snapshot)
        object.__setattr__(self, "policy_sha256", _required_sha256(
            "policy_sha256", self.policy_sha256
        ))
        for name in ("parser_revision", "schema_revision"):
            object.__setattr__(self, name, _required_text(name, getattr(self, name)))
        frozen = freeze_historical_targets(
            self.targets, registry_snapshot_id=snapshot
        )
        object.__setattr__(self, "targets", frozen)
        target_hash = canonical_sha256(
            [item.identity_payload() for item in frozen]
        )
        if _required_sha256("target_sha256", self.target_sha256) != target_hash:
            raise BackfillStateError("target_sha256 does not bind the frozen targets")
        object.__setattr__(self, "target_sha256", target_hash)
        expected_id = stable_campaign_id(
            registry_snapshot_id=snapshot,
            policy_sha256=self.policy_sha256,
            parser_revision=self.parser_revision,
            schema_revision=self.schema_revision,
            targets=frozen,
        )
        if _required_sha256("campaign_id", self.campaign_id) != expected_id:
            raise BackfillStateError("campaign_id does not bind campaign identity")
        object.__setattr__(self, "campaign_id", expected_id)
        try:
            object.__setattr__(self, "status", CampaignStatus(self.status))
        except ValueError as exc:
            raise BackfillStateError("invalid campaign status") from exc
        created = _utc_datetime("created_at", self.created_at)
        updated = _utc_datetime("updated_at", self.updated_at)
        if updated < created:
            raise BackfillStateError("updated_at cannot precede created_at")
        object.__setattr__(self, "created_at", created)
        object.__setattr__(self, "updated_at", updated)
        report_uri, report_hash = _optional_artifact(
            "report_uri", self.report_uri, "report_sha256", self.report_sha256
        )
        object.__setattr__(self, "report_uri", report_uri)
        object.__setattr__(self, "report_sha256", report_hash)
        object.__setattr__(self, "revision", _revision(self.revision))
        if self.contract_version != CONTRACT_VERSION:
            raise BackfillStateError("unsupported campaign contract version")
        if self.status is CampaignStatus.COMPLETE and report_uri is None:
            raise BackfillStateError("complete campaign requires a report artifact")
        if self.status is not CampaignStatus.COMPLETE and report_uri is not None:
            raise BackfillStateError("only a complete campaign may bind a report")

    @classmethod
    def build(
        cls,
        *,
        registry_snapshot_id: str,
        policy_sha256: str,
        parser_revision: str,
        schema_revision: str,
        targets: Iterable[HistoricalScopeTarget | Mapping[str, Any]],
        now: datetime,
        status: CampaignStatus = CampaignStatus.WAITING_PREREQUISITE,
    ) -> "BackfillCampaign":
        snapshot = _required_text("registry_snapshot_id", registry_snapshot_id)
        frozen = freeze_historical_targets(
            targets, registry_snapshot_id=snapshot
        )
        created = _utc_datetime("now", now)
        target_hash = canonical_sha256(
            [item.identity_payload() for item in frozen]
        )
        return cls(
            campaign_id=stable_campaign_id(
                registry_snapshot_id=snapshot,
                policy_sha256=policy_sha256,
                parser_revision=parser_revision,
                schema_revision=schema_revision,
                targets=frozen,
            ),
            registry_snapshot_id=snapshot,
            policy_sha256=policy_sha256,
            parser_revision=parser_revision,
            schema_revision=schema_revision,
            targets=frozen,
            target_sha256=target_hash,
            status=status,
            created_at=created,
            updated_at=created,
        )

    def transition(
        self,
        status: CampaignStatus,
        *,
        now: datetime,
        report_uri: str | None = None,
        report_sha256: str | None = None,
    ) -> "BackfillCampaign":
        requested = CampaignStatus(status)
        allowed = {
            CampaignStatus.WAITING_PREREQUISITE: {
                CampaignStatus.ACTIVE,
                CampaignStatus.BLOCKED_PLATFORM,
            },
            CampaignStatus.ACTIVE: {
                CampaignStatus.BLOCKED_PLATFORM,
                CampaignStatus.COMPLETE,
            },
            CampaignStatus.BLOCKED_PLATFORM: {CampaignStatus.ACTIVE},
            CampaignStatus.COMPLETE: set(),
        }
        if requested not in allowed[self.status]:
            raise BackfillStateError(
                f"invalid campaign transition {self.status.value}->{requested.value}"
            )
        return replace(
            self,
            status=requested,
            updated_at=_utc_datetime("now", now),
            report_uri=report_uri,
            report_sha256=report_sha256,
            revision=self.revision + 1,
        )


@dataclass(frozen=True)
class BackfillScopeState:
    campaign_id: str
    target: HistoricalScopeTarget
    status: ScopeStatus
    attempt_count: int
    source_attempt_count: int
    source_error_count: int
    claim_generation: int
    unavailable_confirmation_days: tuple[date, ...]
    next_retry_at: datetime | None
    batch_id: str | None
    lease_id: str | None
    lease_owner: str | None
    leased_at: datetime | None
    heartbeat_at: datetime | None
    checkpoint_uri: str | None
    checkpoint_sha256: str | None
    scope_manifest_uri: str | None
    scope_manifest_sha256: str | None
    raw_evidence_ids: tuple[str, ...]
    last_error_class: str | None
    last_error_message: str | None
    updated_at: datetime
    revision: int = 0
    contract_version: int = CONTRACT_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "campaign_id", _required_sha256(
            "campaign_id", self.campaign_id
        ))
        if not isinstance(self.target, HistoricalScopeTarget):
            raise BackfillStateError("scope target must be HistoricalScopeTarget")
        try:
            status = ScopeStatus(self.status)
        except ValueError as exc:
            raise BackfillStateError("invalid scope status") from exc
        object.__setattr__(self, "status", status)
        attempts = _non_negative_int("attempt_count", self.attempt_count)
        source_attempts = _non_negative_int(
            "source_attempt_count", self.source_attempt_count
        )
        source_errors = _non_negative_int(
            "source_error_count", self.source_error_count
        )
        claim_generation = _non_negative_int(
            "claim_generation", self.claim_generation
        )
        if source_attempts > attempts:
            raise BackfillStateError("source_attempt_count is inconsistent")
        if source_errors > source_attempts or source_attempts > MAX_SOURCE_ATTEMPTS:
            raise BackfillStateError("source attempt counters are inconsistent")
        object.__setattr__(self, "attempt_count", attempts)
        object.__setattr__(self, "source_attempt_count", source_attempts)
        object.__setattr__(self, "source_error_count", source_errors)
        object.__setattr__(self, "claim_generation", claim_generation)
        days = tuple(sorted(set(self.unavailable_confirmation_days)))
        if any(not isinstance(item, date) or isinstance(item, datetime) for item in days):
            raise BackfillStateError(
                "unavailable confirmation days must be UTC dates"
            )
        object.__setattr__(self, "unavailable_confirmation_days", days)
        object.__setattr__(self, "next_retry_at", _optional_utc_datetime(
            "next_retry_at", self.next_retry_at
        ))
        for name in ("batch_id", "lease_id", "lease_owner"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _required_text(name, value))
        for name in ("leased_at", "heartbeat_at"):
            object.__setattr__(self, name, _optional_utc_datetime(
                name, getattr(self, name)
            ))
        checkpoint_uri, checkpoint_hash = _optional_artifact(
            "checkpoint_uri",
            self.checkpoint_uri,
            "checkpoint_sha256",
            self.checkpoint_sha256,
        )
        manifest_uri, manifest_hash = _optional_artifact(
            "scope_manifest_uri",
            self.scope_manifest_uri,
            "scope_manifest_sha256",
            self.scope_manifest_sha256,
        )
        object.__setattr__(self, "checkpoint_uri", checkpoint_uri)
        object.__setattr__(self, "checkpoint_sha256", checkpoint_hash)
        object.__setattr__(self, "scope_manifest_uri", manifest_uri)
        object.__setattr__(self, "scope_manifest_sha256", manifest_hash)
        evidence = _normalise_raw_evidence(self.raw_evidence_ids)
        object.__setattr__(self, "raw_evidence_ids", evidence)
        for name in ("last_error_class", "last_error_message"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _required_text(name, value))
        object.__setattr__(self, "updated_at", _utc_datetime(
            "updated_at", self.updated_at
        ))
        object.__setattr__(self, "revision", _revision(self.revision))
        if self.contract_version != CONTRACT_VERSION:
            raise BackfillStateError("unsupported scope contract version")
        lease_values = (
            self.lease_id,
            self.lease_owner,
            self.leased_at,
            self.heartbeat_at,
        )
        if status is ScopeStatus.RUNNING:
            if any(item is None for item in lease_values) or self.batch_id is None:
                raise BackfillStateError("running scope requires a complete lease")
            if self.heartbeat_at < self.leased_at:
                raise BackfillStateError("heartbeat_at cannot precede leased_at")
        elif any(item is not None for item in lease_values):
            raise BackfillStateError("non-running scope cannot retain a lease")
        if status in TERMINAL_SCOPE_STATUSES and self.next_retry_at is not None:
            raise BackfillStateError("terminal scope cannot have next_retry_at")
        if status is ScopeStatus.COMPLETE and (
            manifest_uri is None or not evidence
        ):
            raise BackfillStateError(
                "complete scope requires manifest and raw evidence"
            )
        if status is ScopeStatus.UNAVAILABLE and (
            len(days) < UNAVAILABLE_CONFIRMATIONS_REQUIRED
            or source_attempts < UNAVAILABLE_CONFIRMATIONS_REQUIRED
            or not evidence
        ):
            raise BackfillStateError(
                "unavailable scope requires two UTC-day confirmations and evidence"
            )
        if status is ScopeStatus.TERMINAL_ERROR and (
            source_attempts < MAX_SOURCE_ATTEMPTS or not evidence
        ):
            raise BackfillStateError(
                "terminal source error requires three source attempts and raw evidence"
            )

    @classmethod
    def initial(
        cls,
        campaign: BackfillCampaign,
        target: HistoricalScopeTarget,
        *,
        now: datetime,
    ) -> "BackfillScopeState":
        if target not in campaign.targets:
            raise BackfillStateError("scope target is not in the campaign")
        return cls(
            campaign_id=campaign.campaign_id,
            target=target,
            status=ScopeStatus.PENDING,
            attempt_count=0,
            source_attempt_count=0,
            source_error_count=0,
            claim_generation=0,
            unavailable_confirmation_days=(),
            next_retry_at=None,
            batch_id=None,
            lease_id=None,
            lease_owner=None,
            leased_at=None,
            heartbeat_at=None,
            checkpoint_uri=None,
            checkpoint_sha256=None,
            scope_manifest_uri=None,
            scope_manifest_sha256=None,
            raw_evidence_ids=(),
            last_error_class=None,
            last_error_message=None,
            updated_at=_utc_datetime("now", now),
        )


@dataclass(frozen=True)
class BackfillAttempt:
    attempt_id: str
    campaign_id: str
    batch_id: str
    scope_id: str
    sequence: int
    claim_generation: int
    outcome: AttemptOutcome
    started_at: datetime
    finished_at: datetime
    raw_evidence_ids: tuple[str, ...]
    source_observed_at: datetime | None = None
    error_class: str | None = None
    error_message: str | None = None
    retry_after_seconds: int | None = None
    checkpoint_uri: str | None = None
    checkpoint_sha256: str | None = None
    scope_manifest_uri: str | None = None
    scope_manifest_sha256: str | None = None
    contract_version: int = CONTRACT_VERSION

    def __post_init__(self) -> None:
        campaign = _required_sha256("campaign_id", self.campaign_id)
        object.__setattr__(self, "campaign_id", campaign)
        batch = _required_sha256("batch_id", self.batch_id)
        object.__setattr__(self, "batch_id", batch)
        scope = _required_text("scope_id", self.scope_id)
        object.__setattr__(self, "scope_id", scope)
        sequence = _non_negative_int("sequence", self.sequence)
        if sequence < 1:
            raise BackfillStateError("attempt sequence starts at one")
        claim_generation = _non_negative_int(
            "claim_generation", self.claim_generation
        )
        if claim_generation < 1:
            raise BackfillStateError("attempt claim generation starts at one")
        object.__setattr__(self, "claim_generation", claim_generation)
        expected_id = stable_attempt_id(
            campaign, scope, sequence, claim_generation=claim_generation
        )
        if _required_sha256("attempt_id", self.attempt_id) != expected_id:
            raise BackfillStateError("attempt_id does not bind attempt identity")
        object.__setattr__(self, "attempt_id", expected_id)
        try:
            outcome = AttemptOutcome(self.outcome)
        except ValueError as exc:
            raise BackfillStateError("invalid attempt outcome") from exc
        object.__setattr__(self, "outcome", outcome)
        started = _utc_datetime("started_at", self.started_at)
        finished = _utc_datetime("finished_at", self.finished_at)
        if finished < started:
            raise BackfillStateError("finished_at cannot precede started_at")
        object.__setattr__(self, "started_at", started)
        object.__setattr__(self, "finished_at", finished)
        source_observed = self.source_observed_at
        if source_observed is not None:
            source_observed = _utc_datetime(
                "source_observed_at", source_observed
            )
        if outcome is not AttemptOutcome.PLATFORM_ERROR and source_observed is None:
            raise BackfillStateError(
                "source attempt requires immutable source_observed_at"
            )
        object.__setattr__(self, "source_observed_at", source_observed)
        evidence = _normalise_raw_evidence(self.raw_evidence_ids)
        object.__setattr__(self, "raw_evidence_ids", evidence)
        for name in ("error_class", "error_message"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _required_text(name, value))
        if self.retry_after_seconds is not None:
            object.__setattr__(self, "retry_after_seconds", _non_negative_int(
                "retry_after_seconds", self.retry_after_seconds
            ))
        checkpoint_uri, checkpoint_hash = _optional_artifact(
            "checkpoint_uri",
            self.checkpoint_uri,
            "checkpoint_sha256",
            self.checkpoint_sha256,
        )
        manifest_uri, manifest_hash = _optional_artifact(
            "scope_manifest_uri",
            self.scope_manifest_uri,
            "scope_manifest_sha256",
            self.scope_manifest_sha256,
        )
        object.__setattr__(self, "checkpoint_uri", checkpoint_uri)
        object.__setattr__(self, "checkpoint_sha256", checkpoint_hash)
        object.__setattr__(self, "scope_manifest_uri", manifest_uri)
        object.__setattr__(self, "scope_manifest_sha256", manifest_hash)
        if outcome is not AttemptOutcome.PLATFORM_ERROR and not evidence:
            raise BackfillStateError(
                "source attempt outcome requires raw evidence IDs"
            )
        if outcome in {AttemptOutcome.SOURCE_ERROR, AttemptOutcome.PLATFORM_ERROR}:
            if self.error_class is None:
                raise BackfillStateError("error attempt requires error_class")
        elif self.error_class is not None or self.error_message is not None:
            raise BackfillStateError("successful/availability attempt cannot carry error")
        if self.retry_after_seconds is not None and outcome is not AttemptOutcome.SOURCE_ERROR:
            raise BackfillStateError("Retry-After applies only to source errors")
        if outcome is AttemptOutcome.CAPTURED and manifest_uri is None:
            raise BackfillStateError("captured attempt requires a scope manifest")
        if self.contract_version != CONTRACT_VERSION:
            raise BackfillStateError("unsupported attempt contract version")

    @classmethod
    def build(
        cls,
        *,
        scope: BackfillScopeState,
        batch_id: str,
        outcome: AttemptOutcome,
        started_at: datetime,
        finished_at: datetime,
        raw_evidence_ids: Iterable[str] = (),
        source_observed_at: datetime | None = None,
        error_class: str | None = None,
        error_message: str | None = None,
        retry_after_seconds: int | None = None,
        checkpoint_uri: str | None = None,
        checkpoint_sha256: str | None = None,
        scope_manifest_uri: str | None = None,
        scope_manifest_sha256: str | None = None,
    ) -> "BackfillAttempt":
        sequence = scope.attempt_count + 1
        resolved_outcome = AttemptOutcome(outcome)
        if (
            resolved_outcome is AttemptOutcome.UNAVAILABLE_CONFIRMATION
            and scope.source_attempt_count + 1 >= MAX_SOURCE_ATTEMPTS
            and source_observed_at is not None
            and len({
                *scope.unavailable_confirmation_days,
                _utc_datetime(
                    "source_observed_at", source_observed_at
                ).date(),
            }) < UNAVAILABLE_CONFIRMATIONS_REQUIRED
        ):
            # The final source slot cannot leave a retryable state that no
            # future claim is allowed to execute.  Preserve the raw evidence,
            # but terminalise it as an explicit source error.
            resolved_outcome = AttemptOutcome.SOURCE_ERROR
            error_class = "unavailable_confirmation_exhausted"
            error_message = None
        return cls(
            attempt_id=stable_attempt_id(
                scope.campaign_id,
                scope.target.scope_id,
                sequence,
                claim_generation=scope.claim_generation,
            ),
            campaign_id=scope.campaign_id,
            batch_id=batch_id,
            scope_id=scope.target.scope_id,
            sequence=sequence,
            claim_generation=scope.claim_generation,
            outcome=resolved_outcome,
            started_at=started_at,
            finished_at=finished_at,
            raw_evidence_ids=tuple(raw_evidence_ids),
            source_observed_at=source_observed_at,
            error_class=error_class,
            error_message=error_message,
            retry_after_seconds=retry_after_seconds,
            checkpoint_uri=checkpoint_uri,
            checkpoint_sha256=checkpoint_sha256,
            scope_manifest_uri=scope_manifest_uri,
            scope_manifest_sha256=scope_manifest_sha256,
        )


def stable_attempt_id(
    campaign_id: str,
    scope_id: str,
    sequence: int,
    *,
    claim_generation: int | None = None,
) -> str:
    sequence_value = _non_negative_int("sequence", sequence)
    if sequence_value < 1:
        raise BackfillStateError("attempt sequence starts at one")
    generation = (
        sequence_value
        if claim_generation is None
        else _non_negative_int("claim_generation", claim_generation)
    )
    if generation < 1:
        raise BackfillStateError("attempt claim generation starts at one")
    return canonical_sha256(
        {
            "contract_version": CONTRACT_VERSION,
            "campaign_id": _required_sha256("campaign_id", campaign_id),
            "scope_id": _required_text("scope_id", scope_id),
            "sequence": sequence_value,
            "claim_generation": generation,
        }
    )


@dataclass(frozen=True)
class BackfillPlatformIncident:
    """Immutable evidence for a platform failure at one batch revision."""

    incident_id: str
    campaign_id: str
    batch_id: str
    phase: str
    error_class: str
    blocked_from_status: BatchStatus
    pre_incident_batch_revision: int
    report_uri: str
    report_sha256: str
    raw_evidence_ids: tuple[str, ...]
    created_at: datetime
    contract_version: int = CONTRACT_VERSION

    def __post_init__(self) -> None:
        incident_id = _required_sha256("incident_id", self.incident_id)
        campaign_id = _required_sha256("campaign_id", self.campaign_id)
        batch_id = _required_sha256("batch_id", self.batch_id)
        object.__setattr__(self, "incident_id", incident_id)
        object.__setattr__(self, "campaign_id", campaign_id)
        object.__setattr__(self, "batch_id", batch_id)
        object.__setattr__(self, "phase", _required_text("phase", self.phase))
        object.__setattr__(
            self, "error_class", _required_text("error_class", self.error_class)
        )
        try:
            blocked_from = BatchStatus(self.blocked_from_status)
        except ValueError as exc:
            raise BackfillStateError("invalid incident blocked_from_status") from exc
        if blocked_from is BatchStatus.BLOCKED_PLATFORM:
            raise BackfillStateError(
                "platform incident cannot originate from a blocked batch"
            )
        object.__setattr__(self, "blocked_from_status", blocked_from)
        pre_incident_revision = _revision(self.pre_incident_batch_revision)
        object.__setattr__(
            self, "pre_incident_batch_revision", pre_incident_revision
        )
        object.__setattr__(
            self, "report_uri", _required_text("report_uri", self.report_uri)
        )
        object.__setattr__(
            self,
            "report_sha256",
            _required_sha256("report_sha256", self.report_sha256),
        )
        object.__setattr__(
            self,
            "raw_evidence_ids",
            _normalise_raw_evidence(self.raw_evidence_ids),
        )
        object.__setattr__(
            self, "created_at", _utc_datetime("created_at", self.created_at)
        )
        if self.contract_version != CONTRACT_VERSION:
            raise BackfillStateError(
                "unsupported platform-incident contract version"
            )
        expected_id = _platform_incident_id_for_fields(
            campaign_id=campaign_id,
            batch_id=batch_id,
            pre_incident_batch_revision=pre_incident_revision,
            blocked_from_status=blocked_from,
            phase=self.phase,
            error_class=self.error_class,
            raw_evidence_ids=self.raw_evidence_ids,
        )
        if incident_id != expected_id:
            raise BackfillStateError(
                "incident_id does not bind platform incident content"
            )

    @classmethod
    def build(
        cls,
        batch: "BackfillBatch",
        *,
        phase: str,
        error_class: str,
        report_uri: str,
        report_sha256: str,
        raw_evidence_ids: Iterable[str] = (),
        now: datetime,
    ) -> "BackfillPlatformIncident":
        current = _utc_datetime("now", now)
        normalised_evidence = _normalise_raw_evidence(raw_evidence_ids)
        return cls(
            incident_id=stable_platform_incident_id(
                batch,
                phase=phase,
                error_class=error_class,
                raw_evidence_ids=normalised_evidence,
            ),
            campaign_id=batch.campaign_id,
            batch_id=batch.batch_id,
            phase=phase,
            error_class=error_class,
            blocked_from_status=batch.status,
            pre_incident_batch_revision=batch.revision,
            report_uri=report_uri,
            report_sha256=report_sha256,
            raw_evidence_ids=normalised_evidence,
            created_at=current,
        )


def _platform_incident_id_for_fields(
    *,
    campaign_id: str,
    batch_id: str,
    pre_incident_batch_revision: int,
    blocked_from_status: BatchStatus,
    phase: str,
    error_class: str,
    raw_evidence_ids: Iterable[str] = (),
) -> str:
    try:
        blocked_from = BatchStatus(blocked_from_status)
    except ValueError as exc:
        raise BackfillStateError("invalid incident blocked_from_status") from exc
    if blocked_from is BatchStatus.BLOCKED_PLATFORM:
        raise BackfillStateError(
            "platform incident cannot originate from a blocked batch"
        )
    return canonical_sha256(
        {
            "contract_version": CONTRACT_VERSION,
            "campaign_id": _required_sha256("campaign_id", campaign_id),
            "batch_id": _required_sha256("batch_id", batch_id),
            "pre_incident_batch_revision": _revision(
                pre_incident_batch_revision
            ),
            "blocked_from_status": blocked_from,
            "phase": _required_text("phase", phase),
            "error_class": _required_text("error_class", error_class),
            "raw_evidence_ids": _normalise_raw_evidence(raw_evidence_ids),
        }
    )


def stable_platform_incident_id(
    batch: "BackfillBatch",
    *,
    phase: str,
    error_class: str,
    raw_evidence_ids: Iterable[str] = (),
) -> str:
    """Bind an incident to its exact pre-incident batch CAS revision."""

    if not isinstance(batch, BackfillBatch):
        raise BackfillStateError("platform incident requires a batch record")
    if batch.status is BatchStatus.BLOCKED_PLATFORM:
        raise BackfillStateError(
            "platform incident cannot originate from a blocked batch"
        )
    return _platform_incident_id_for_fields(
        campaign_id=batch.campaign_id,
        batch_id=batch.batch_id,
        pre_incident_batch_revision=batch.revision,
        blocked_from_status=batch.status,
        phase=phase,
        error_class=error_class,
        raw_evidence_ids=raw_evidence_ids,
    )


@dataclass(frozen=True)
class BackfillBatch:
    batch_id: str
    campaign_id: str
    scope_ids: tuple[str, ...]
    scope_claim_generations: tuple[int, ...]
    status: BatchStatus
    claimed_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None
    snapshot_pins: Mapping[str, str | int] | None = None
    dq_report_uri: str | None = None
    dq_report_sha256: str | None = None
    raw_evidence_ids: tuple[str, ...] = ()
    platform_incidents: tuple[BackfillPlatformIncident, ...] = ()
    open_platform_incident_id: str | None = None
    revision: int = 0
    contract_version: int = CONTRACT_VERSION

    def __post_init__(self) -> None:
        campaign = _required_sha256("campaign_id", self.campaign_id)
        object.__setattr__(self, "campaign_id", campaign)
        scopes = tuple(_required_text("scope_id", item) for item in self.scope_ids)
        if not scopes or len(scopes) > MAX_SCOPE_BATCH:
            raise BackfillStateError(
                f"batch must contain 1..{MAX_SCOPE_BATCH} scopes"
            )
        if len(scopes) != len(set(scopes)):
            raise BackfillStateError("batch contains duplicate scope_id")
        object.__setattr__(self, "scope_ids", scopes)
        generations = tuple(
            _non_negative_int("scope claim generation", item)
            for item in self.scope_claim_generations
        )
        if len(generations) != len(scopes) or any(item < 1 for item in generations):
            raise BackfillStateError(
                "batch scope claim generations must align and start at one"
            )
        object.__setattr__(self, "scope_claim_generations", generations)
        expected = stable_batch_id(campaign, scopes, generations)
        if _required_sha256("batch_id", self.batch_id) != expected:
            raise BackfillStateError("batch_id does not bind ordered scope_ids")
        object.__setattr__(self, "batch_id", expected)
        try:
            status = BatchStatus(self.status)
        except ValueError as exc:
            raise BackfillStateError("invalid batch status") from exc
        object.__setattr__(self, "status", status)
        claimed = _utc_datetime("claimed_at", self.claimed_at)
        updated = _utc_datetime("updated_at", self.updated_at)
        completed = _optional_utc_datetime("completed_at", self.completed_at)
        if updated < claimed or (completed is not None and completed < claimed):
            raise BackfillStateError("batch timestamps are inconsistent")
        object.__setattr__(self, "claimed_at", claimed)
        object.__setattr__(self, "updated_at", updated)
        object.__setattr__(self, "completed_at", completed)
        if self.snapshot_pins is not None:
            if not isinstance(self.snapshot_pins, Mapping):
                raise BackfillStateError("snapshot_pins must be a mapping")
            pins: dict[str, str | int] = {}
            for key, value in self.snapshot_pins.items():
                name = _required_text("snapshot pin name", key)
                if isinstance(value, bool) or not isinstance(value, (str, int)):
                    raise BackfillStateError(
                        "snapshot pin must be a non-empty string or integer"
                    )
                if isinstance(value, str):
                    value = _required_text("snapshot pin", value)
                elif value < 0:
                    raise BackfillStateError("snapshot pin must be non-negative")
                pins[name] = value
            object.__setattr__(self, "snapshot_pins", dict(sorted(pins.items())))
        dq_uri, dq_hash = _optional_artifact(
            "dq_report_uri",
            self.dq_report_uri,
            "dq_report_sha256",
            self.dq_report_sha256,
        )
        object.__setattr__(self, "dq_report_uri", dq_uri)
        object.__setattr__(self, "dq_report_sha256", dq_hash)
        object.__setattr__(self, "raw_evidence_ids", _normalise_raw_evidence(
            self.raw_evidence_ids
        ))
        revision = _revision(self.revision)
        object.__setattr__(self, "revision", revision)
        incidents = tuple(self.platform_incidents)
        if any(not isinstance(item, BackfillPlatformIncident) for item in incidents):
            raise BackfillStateError(
                "platform_incidents must contain platform incident records"
            )
        incident_ids = tuple(item.incident_id for item in incidents)
        if len(incident_ids) != len(set(incident_ids)):
            raise BackfillStateError("batch contains duplicate platform incident_id")
        previous_incident_revision: int | None = None
        for incident in incidents:
            if (
                incident.campaign_id != campaign
                or incident.batch_id != expected
            ):
                raise BackfillStateError(
                    "platform incident belongs to another batch"
                )
            if (
                previous_incident_revision is not None
                and incident.pre_incident_batch_revision
                < previous_incident_revision + 2
            ):
                raise BackfillStateError(
                    "platform incident history has inconsistent revisions"
                )
            if incident.pre_incident_batch_revision >= revision:
                raise BackfillStateError(
                    "platform incident revision is not before batch revision"
                )
            previous_incident_revision = incident.pre_incident_batch_revision
        object.__setattr__(self, "platform_incidents", incidents)
        open_incident_id = self.open_platform_incident_id
        if open_incident_id is not None:
            open_incident_id = _required_sha256(
                "open_platform_incident_id", open_incident_id
            )
            if open_incident_id not in incident_ids:
                raise BackfillStateError(
                    "open platform incident is absent from batch history"
                )
            if not incidents or incidents[-1].incident_id != open_incident_id:
                raise BackfillStateError(
                    "open platform incident must be the latest incident"
                )
            open_incident = incidents[-1]
            if revision != open_incident.pre_incident_batch_revision + 1:
                raise BackfillStateError(
                    "open platform incident does not bind prior batch revision"
                )
            if open_incident.blocked_from_status is BatchStatus.COMPLETE:
                if status is not BatchStatus.COMPLETE:
                    raise BackfillStateError(
                        "complete-batch incident must leave batch complete"
                    )
            elif status is not BatchStatus.BLOCKED_PLATFORM:
                raise BackfillStateError(
                    "open platform incident requires a blocked batch"
                )
        object.__setattr__(
            self, "open_platform_incident_id", open_incident_id
        )
        if (
            open_incident_id is None
            and incidents
            and revision < incidents[-1].pre_incident_batch_revision + 2
        ):
            raise BackfillStateError(
                "resolved platform incident lacks a resolution revision"
            )
        if self.contract_version != CONTRACT_VERSION:
            raise BackfillStateError("unsupported batch contract version")
        if status is BatchStatus.COMPLETE and (
            completed is None
            or dq_uri is None
            or not self.snapshot_pins
            or not self.raw_evidence_ids
        ):
            raise BackfillStateError(
                "complete batch requires pins, raw evidence, DQ report, and completed_at"
            )
        if status is not BatchStatus.COMPLETE and completed is not None:
            raise BackfillStateError("only a complete batch may have completed_at")

    @classmethod
    def build(
        cls,
        campaign_id: str,
        scope_ids: Sequence[str],
        *,
        scope_claim_generations: Sequence[int] | None = None,
        now: datetime,
    ) -> "BackfillBatch":
        scopes = tuple(scope_ids)
        generations = tuple(
            scope_claim_generations
            if scope_claim_generations is not None
            else (1 for _item in scopes)
        )
        claimed = _utc_datetime("now", now)
        return cls(
            batch_id=stable_batch_id(campaign_id, scopes, generations),
            campaign_id=campaign_id,
            scope_ids=scopes,
            scope_claim_generations=generations,
            status=BatchStatus.CLAIMED,
            claimed_at=claimed,
            updated_at=claimed,
        )

    def transition(
        self,
        status: BatchStatus,
        *,
        now: datetime,
        snapshot_pins: Mapping[str, str | int] | None = None,
        dq_report_uri: str | None = None,
        dq_report_sha256: str | None = None,
        raw_evidence_ids: Iterable[str] | None = None,
    ) -> "BackfillBatch":
        if self.open_platform_incident_id is not None:
            raise BackfillStateError(
                "open platform incident must be resolved explicitly"
            )
        requested = BatchStatus(status)
        allowed = {
            BatchStatus.CLAIMED: {
                BatchStatus.RUNNING,
                BatchStatus.BLOCKED_PLATFORM,
            },
            BatchStatus.RUNNING: {
                BatchStatus.DQ_PENDING,
                BatchStatus.BLOCKED_PLATFORM,
            },
            BatchStatus.DQ_PENDING: {
                BatchStatus.COMPLETE,
                BatchStatus.BLOCKED_PLATFORM,
            },
            BatchStatus.BLOCKED_PLATFORM: {BatchStatus.RUNNING},
            BatchStatus.COMPLETE: set(),
        }
        if requested not in allowed[self.status]:
            raise BackfillStateError(
                f"invalid batch transition {self.status.value}->{requested.value}"
            )
        current = _utc_datetime("now", now)
        return replace(
            self,
            status=requested,
            updated_at=current,
            completed_at=(current if requested is BatchStatus.COMPLETE else None),
            snapshot_pins=(
                snapshot_pins if snapshot_pins is not None else self.snapshot_pins
            ),
            dq_report_uri=(
                dq_report_uri if dq_report_uri is not None else self.dq_report_uri
            ),
            dq_report_sha256=(
                dq_report_sha256
                if dq_report_sha256 is not None
                else self.dq_report_sha256
            ),
            raw_evidence_ids=(
                tuple(raw_evidence_ids)
                if raw_evidence_ids is not None
                else self.raw_evidence_ids
            ),
            revision=self.revision + 1,
        )

    def record_platform_incident(
        self,
        incident: BackfillPlatformIncident,
        *,
        now: datetime,
    ) -> "BackfillBatch":
        """Append and open an incident fenced to this exact batch revision."""

        if not isinstance(incident, BackfillPlatformIncident):
            raise BackfillStateError("platform incident record is required")
        if self.open_platform_incident_id is not None:
            raise BackfillStateError("batch already has an open platform incident")
        if incident.incident_id in {
            item.incident_id for item in self.platform_incidents
        }:
            raise BackfillStateError("platform incident_id is already recorded")
        if (
            incident.campaign_id != self.campaign_id
            or incident.batch_id != self.batch_id
        ):
            raise BackfillStateError("platform incident belongs to another batch")
        if incident.blocked_from_status is not self.status:
            raise BackfillStateError(
                "platform incident does not bind current batch status"
            )
        if incident.pre_incident_batch_revision != self.revision:
            raise BackfillStateError(
                "platform incident does not bind current batch revision"
            )
        expected_id = stable_platform_incident_id(
            self,
            phase=incident.phase,
            error_class=incident.error_class,
            raw_evidence_ids=incident.raw_evidence_ids,
        )
        if incident.incident_id != expected_id:
            raise BackfillStateError(
                "platform incident_id does not bind current batch revision"
            )
        current = _utc_datetime("now", now)
        if current < self.updated_at or current < incident.created_at:
            raise BackfillStateError("platform incident timestamps are inconsistent")
        next_status = (
            BatchStatus.COMPLETE
            if self.status is BatchStatus.COMPLETE
            else BatchStatus.BLOCKED_PLATFORM
        )
        return replace(
            self,
            status=next_status,
            updated_at=current,
            platform_incidents=(*self.platform_incidents, incident),
            open_platform_incident_id=incident.incident_id,
            revision=self.revision + 1,
        )

    def resolve_platform_incident(self, *, now: datetime) -> "BackfillBatch":
        """Close the current incident and restore the exact recoverable phase."""

        if self.open_platform_incident_id is None:
            raise BackfillStateError("batch has no open platform incident")
        incident = next(
            item
            for item in self.platform_incidents
            if item.incident_id == self.open_platform_incident_id
        )
        if self.status is BatchStatus.COMPLETE:
            restored = BatchStatus.COMPLETE
        elif self.status is BatchStatus.BLOCKED_PLATFORM:
            restored = incident.blocked_from_status
            if restored is BatchStatus.CLAIMED:
                restored = BatchStatus.RUNNING
        else:  # guarded by BackfillBatch validation, kept fail-closed for clarity
            raise BackfillStateError(
                "open platform incident is not in a resolvable batch state"
            )
        current = _utc_datetime("now", now)
        if current < self.updated_at:
            raise BackfillStateError("platform incident resolution moves backwards")
        return replace(
            self,
            status=restored,
            updated_at=current,
            open_platform_incident_id=None,
            revision=self.revision + 1,
        )


@dataclass(frozen=True)
class ClaimResult:
    batch: BackfillBatch | None
    scopes: tuple[BackfillScopeState, ...]
    reclaimed_scope_ids: tuple[str, ...]
    reclaimed_scopes: tuple[BackfillScopeState, ...]


def is_stale_lease(scope: BackfillScopeState, *, now: datetime) -> bool:
    current = _utc_datetime("now", now)
    if scope.status is not ScopeStatus.RUNNING:
        return False
    assert scope.heartbeat_at is not None
    return current >= scope.heartbeat_at + STALE_LEASE_AFTER


def reclaim_stale_lease(
    scope: BackfillScopeState, *, now: datetime
) -> BackfillScopeState:
    current = _utc_datetime("now", now)
    if not is_stale_lease(scope, now=current):
        raise BackfillStateError("scope lease is not stale")
    return replace(
        scope,
        status=ScopeStatus.PENDING,
        next_retry_at=None,
        lease_id=None,
        lease_owner=None,
        leased_at=None,
        heartbeat_at=None,
        last_error_class="stale_lease_reclaimed",
        last_error_message=None,
        updated_at=current,
        revision=scope.revision + 1,
    )


def heartbeat_scope(
    scope: BackfillScopeState,
    *,
    lease_id: str,
    now: datetime,
) -> BackfillScopeState:
    current = _utc_datetime("now", now)
    if scope.status is not ScopeStatus.RUNNING:
        raise BackfillStateError("only a running scope can heartbeat")
    if scope.lease_id != _required_text("lease_id", lease_id):
        raise BackfillStateError("lease_id does not own the scope")
    if current < scope.heartbeat_at:
        raise BackfillStateError("heartbeat cannot move backwards")
    return replace(
        scope,
        heartbeat_at=current,
        updated_at=current,
        revision=scope.revision + 1,
    )


def recover_batch_scopes(
    batch: BackfillBatch,
    scopes: Iterable[BackfillScopeState],
    *,
    lease_owner: str,
    now: datetime,
) -> tuple[BackfillScopeState, ...]:
    """Finish or refresh an interrupted durable claim without a new batch.

    A batch row is inserted before its individual scope CAS operations.  A
    scheduler crash can therefore leave only part of the membership leased.
    Recovery keeps the original claim generation and batch identity, leases
    the missing members, and heartbeats members that are still running.  Scope
    attempts already accounted by the finalizer are returned unchanged.
    """

    if batch.status not in {BatchStatus.CLAIMED, BatchStatus.RUNNING}:
        raise BackfillStateError("only a claimed/running batch can recover scopes")
    owner = _required_text("lease_owner", lease_owner)
    current = _utc_datetime("now", now)
    supplied = tuple(scopes)
    by_id = {item.target.scope_id: item for item in supplied}
    if len(by_id) != len(supplied) or set(by_id) != set(batch.scope_ids):
        raise BackfillStateError("batch recovery requires its exact scope membership")
    expected_generation = dict(zip(
        batch.scope_ids,
        batch.scope_claim_generations,
        strict=True,
    ))
    running = tuple(
        item for item in supplied if item.status is ScopeStatus.RUNNING
    )
    running_lease_ids = {item.lease_id for item in running}
    running_owners = {item.lease_owner for item in running}
    if len(running_lease_ids) > 1 or len(running_owners) > 1:
        raise BackfillStateError("batch recovery found inconsistent running leases")
    lease_id = (
        next(iter(running_lease_ids))
        if running_lease_ids
        else canonical_sha256({
            "contract_version": CONTRACT_VERSION,
            "batch_id": batch.batch_id,
            "purpose": "batch-claim-recovery",
        })
    )
    assert lease_id is not None
    recovered: list[BackfillScopeState] = []
    for scope_id in batch.scope_ids:
        scope = by_id[scope_id]
        generation = expected_generation[scope_id]
        if scope.status is ScopeStatus.RUNNING:
            if scope.batch_id != batch.batch_id or scope.claim_generation != generation:
                raise BackfillStateError("running scope differs from batch claim")
            recovered.append(heartbeat_scope(
                scope,
                lease_id=lease_id,
                now=current,
            ))
            continue
        if scope.batch_id == batch.batch_id and scope.claim_generation == generation:
            # The finalizer already persisted this generation's attempt.
            recovered.append(scope)
            continue
        if (
            scope.status not in {ScopeStatus.PENDING, ScopeStatus.RETRYABLE_ERROR}
            or scope.claim_generation + 1 != generation
        ):
            raise BackfillStateError("scope cannot be joined to interrupted batch claim")
        recovered.append(replace(
            scope,
            status=ScopeStatus.RUNNING,
            batch_id=batch.batch_id,
            lease_id=lease_id,
            lease_owner=(next(iter(running_owners)) if running_owners else owner),
            leased_at=current,
            heartbeat_at=current,
            next_retry_at=None,
            claim_generation=generation,
            updated_at=current,
            revision=scope.revision + 1,
        ))
    return tuple(recovered)


def resume_platform_scopes(
    batch: BackfillBatch,
    scopes: Iterable[BackfillScopeState],
    *,
    platform_scope_ids: Iterable[str],
    lease_owner: str,
    now: datetime,
) -> tuple[BackfillScopeState, ...]:
    """Explicitly reopen platform-failed scopes inside their fenced batch."""

    if batch.status not in {BatchStatus.BLOCKED_PLATFORM, BatchStatus.RUNNING}:
        raise BackfillStateError("platform resume requires a blocked/running batch")
    owner = _required_text("lease_owner", lease_owner)
    current = _utc_datetime("now", now)
    supplied = tuple(scopes)
    by_id = {item.target.scope_id: item for item in supplied}
    if len(by_id) != len(supplied) or set(by_id) != set(batch.scope_ids):
        raise BackfillStateError("platform resume requires exact batch membership")
    retry_ids = frozenset(_required_text("scope_id", item) for item in platform_scope_ids)
    if not retry_ids or not retry_ids <= set(batch.scope_ids):
        raise BackfillStateError("platform resume scope set is invalid")
    expected_generation = dict(zip(
        batch.scope_ids,
        batch.scope_claim_generations,
        strict=True,
    ))
    lease_id = canonical_sha256({
        "contract_version": CONTRACT_VERSION,
        "batch_id": batch.batch_id,
        "purpose": "explicit-platform-resume",
        "attempt_counts": [
            (scope_id, by_id[scope_id].attempt_count)
            for scope_id in sorted(retry_ids)
        ],
    })
    resumed: list[BackfillScopeState] = []
    for scope_id in batch.scope_ids:
        scope = by_id[scope_id]
        if (
            scope.batch_id != batch.batch_id
            or scope.claim_generation != expected_generation[scope_id]
        ):
            raise BackfillStateError("platform resume scope identity drifted")
        should_run = scope.status is ScopeStatus.RUNNING or scope_id in retry_ids
        if scope_id in retry_ids and scope.status not in {
            ScopeStatus.RETRYABLE_ERROR,
            ScopeStatus.RUNNING,
        }:
            raise BackfillStateError("platform-failed scope is not resumable")
        if not should_run:
            resumed.append(scope)
            continue
        resumed.append(replace(
            scope,
            status=ScopeStatus.RUNNING,
            lease_id=lease_id,
            lease_owner=owner,
            leased_at=current,
            heartbeat_at=current,
            next_retry_at=None,
            updated_at=current,
            revision=scope.revision + 1,
        ))
    return tuple(resumed)


def _claim_sort_key(scope: BackfillScopeState) -> tuple[Any, ...]:
    if scope.status is ScopeStatus.RETRYABLE_ERROR:
        return (0, scope.next_retry_at or datetime.min.replace(tzinfo=timezone.utc), scope.target.scope_id)
    return (1, datetime.min.replace(tzinfo=timezone.utc), scope.target.scope_id)


def claim_scopes(
    campaign: BackfillCampaign,
    scopes: Iterable[BackfillScopeState],
    *,
    lease_owner: str,
    now: datetime,
    limit: int = MAX_SCOPE_BATCH,
) -> ClaimResult:
    """Reclaim stale work and deterministically lease at most one bounded batch."""

    if campaign.status is not CampaignStatus.ACTIVE:
        raise BackfillStateError("only an active campaign may claim scopes")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_SCOPE_BATCH:
        raise BackfillStateError(
            f"claim limit must be in 1..{MAX_SCOPE_BATCH}"
        )
    owner = _required_text("lease_owner", lease_owner)
    current = _utc_datetime("now", now)
    supplied = tuple(scopes)
    accounting_for(campaign, supplied)
    reclaimed: list[str] = []
    reclaimed_records: list[BackfillScopeState] = []
    normalised: list[BackfillScopeState] = []
    for scope in supplied:
        if is_stale_lease(scope, now=current):
            scope = reclaim_stale_lease(scope, now=current)
            reclaimed.append(scope.target.scope_id)
            reclaimed_records.append(scope)
        normalised.append(scope)
    ready = [
        scope for scope in normalised
        if scope.status is ScopeStatus.PENDING
        or (
            scope.status is ScopeStatus.RETRYABLE_ERROR
            and scope.next_retry_at is not None
            and scope.next_retry_at <= current
        )
    ]
    chosen = tuple(sorted(ready, key=_claim_sort_key)[:limit])
    if not chosen:
        return ClaimResult(
            batch=None,
            scopes=tuple(sorted(normalised, key=lambda item: item.target.scope_id)),
            reclaimed_scope_ids=tuple(sorted(reclaimed)),
            reclaimed_scopes=tuple(sorted(
                reclaimed_records, key=lambda item: item.target.scope_id
            )),
        )
    scope_ids = tuple(item.target.scope_id for item in chosen)
    claim_generations = tuple(item.claim_generation + 1 for item in chosen)
    batch = BackfillBatch.build(
        campaign.campaign_id,
        scope_ids,
        scope_claim_generations=claim_generations,
        now=current,
    )
    lease_id = canonical_sha256(
        {
            "contract_version": CONTRACT_VERSION,
            "batch_id": batch.batch_id,
            "lease_owner": owner,
            "claimed_at": current,
        }
    )
    selected = set(scope_ids)
    updated: list[BackfillScopeState] = []
    for scope in normalised:
        if scope.target.scope_id in selected:
            scope = replace(
                scope,
                status=ScopeStatus.RUNNING,
                batch_id=batch.batch_id,
                lease_id=lease_id,
                lease_owner=owner,
                leased_at=current,
                heartbeat_at=current,
                next_retry_at=None,
                claim_generation=scope.claim_generation + 1,
                updated_at=current,
                revision=scope.revision + 1,
            )
        updated.append(scope)
    return ClaimResult(
        batch=batch,
        scopes=tuple(sorted(updated, key=lambda item: item.target.scope_id)),
        reclaimed_scope_ids=tuple(sorted(reclaimed)),
        reclaimed_scopes=tuple(sorted(
            reclaimed_records, key=lambda item: item.target.scope_id
        )),
    )


def apply_attempt(
    scope: BackfillScopeState,
    attempt: BackfillAttempt,
) -> BackfillScopeState:
    """Apply the exact unavailable/source retry policy to one leased scope."""

    if scope.status is not ScopeStatus.RUNNING:
        raise BackfillStateError("attempt can finish only a running scope")
    if attempt.campaign_id != scope.campaign_id:
        raise BackfillStateError("attempt belongs to another campaign")
    if attempt.scope_id != scope.target.scope_id:
        raise BackfillStateError("attempt belongs to another scope")
    if attempt.batch_id != scope.batch_id:
        raise BackfillStateError("attempt belongs to another batch")
    if attempt.sequence != scope.attempt_count + 1:
        raise BackfillStateError("attempt sequence is not the next exact value")
    if attempt.claim_generation != scope.claim_generation:
        raise BackfillStateError("attempt belongs to another claim generation")
    if (
        attempt.outcome is not AttemptOutcome.PLATFORM_ERROR
        and scope.source_attempt_count >= MAX_SOURCE_ATTEMPTS
    ):
        raise BackfillStateError("scope source-attempt limit is exhausted")
    evidence = _normalise_raw_evidence(
        (*scope.raw_evidence_ids, *attempt.raw_evidence_ids)
    )
    common: dict[str, Any] = {
        "attempt_count": attempt.sequence,
        "raw_evidence_ids": evidence,
        "checkpoint_uri": attempt.checkpoint_uri or scope.checkpoint_uri,
        "checkpoint_sha256": (
            attempt.checkpoint_sha256 or scope.checkpoint_sha256
        ),
        "scope_manifest_uri": (
            attempt.scope_manifest_uri or scope.scope_manifest_uri
        ),
        "scope_manifest_sha256": (
            attempt.scope_manifest_sha256 or scope.scope_manifest_sha256
        ),
        "lease_id": None,
        "lease_owner": None,
        "leased_at": None,
        "heartbeat_at": None,
        "updated_at": attempt.finished_at,
        "revision": scope.revision + 1,
    }
    if attempt.outcome is AttemptOutcome.CAPTURED:
        return replace(
            scope,
            **common,
            status=ScopeStatus.CAPTURED_PENDING_DQ,
            source_attempt_count=scope.source_attempt_count + 1,
            next_retry_at=None,
            last_error_class=None,
            last_error_message=None,
        )
    if attempt.outcome is AttemptOutcome.UNAVAILABLE_CONFIRMATION:
        assert attempt.source_observed_at is not None
        confirmation_day = attempt.source_observed_at.date()
        if confirmation_day in scope.unavailable_confirmation_days:
            raise BackfillStateError(
                "unavailable confirmation must use a distinct UTC day"
            )
        source_attempts = scope.source_attempt_count + 1
        days = tuple(sorted({
            *scope.unavailable_confirmation_days,
            confirmation_day,
        }))
        if len(days) >= UNAVAILABLE_CONFIRMATIONS_REQUIRED:
            return replace(
                scope,
                **common,
                status=ScopeStatus.UNAVAILABLE,
                source_attempt_count=source_attempts,
                unavailable_confirmation_days=days,
                next_retry_at=None,
                last_error_class=None,
                last_error_message=None,
            )
        next_day = datetime.combine(
            confirmation_day + timedelta(days=1),
            time.min,
            tzinfo=timezone.utc,
        )
        return replace(
            scope,
            **common,
            status=ScopeStatus.RETRYABLE_ERROR,
            source_attempt_count=source_attempts,
            unavailable_confirmation_days=days,
            next_retry_at=max(attempt.finished_at, next_day),
            last_error_class="unavailable_confirmation_pending",
            last_error_message=None,
        )
    if attempt.outcome is AttemptOutcome.SOURCE_ERROR:
        source_attempts = scope.source_attempt_count + 1
        source_errors = scope.source_error_count + 1
        if source_attempts >= MAX_SOURCE_ATTEMPTS:
            return replace(
                scope,
                **common,
                status=ScopeStatus.TERMINAL_ERROR,
                source_attempt_count=source_attempts,
                source_error_count=source_errors,
                next_retry_at=None,
                last_error_class=attempt.error_class,
                last_error_message=attempt.error_message,
            )
        base_delay = (
            FIRST_SOURCE_RETRY_DELAY
            if source_errors == 1
            else SECOND_SOURCE_RETRY_DELAY
        )
        retry_after = timedelta(seconds=attempt.retry_after_seconds or 0)
        return replace(
            scope,
            **common,
            status=ScopeStatus.RETRYABLE_ERROR,
            source_attempt_count=source_attempts,
            source_error_count=source_errors,
            next_retry_at=attempt.finished_at + max(base_delay, retry_after),
            last_error_class=attempt.error_class,
            last_error_message=attempt.error_message,
        )
    return replace(
        scope,
        **common,
        status=ScopeStatus.RETRYABLE_ERROR,
        next_retry_at=None,
        last_error_class=attempt.error_class,
        last_error_message=attempt.error_message,
    )


def attempt_blocks_campaign(attempt: BackfillAttempt) -> bool:
    return attempt.outcome is AttemptOutcome.PLATFORM_ERROR


def mark_scope_dq_complete(
    scope: BackfillScopeState,
    *,
    scope_manifest_uri: str,
    scope_manifest_sha256: str,
    raw_evidence_ids: Iterable[str] = (),
    now: datetime,
) -> BackfillScopeState:
    if scope.status is not ScopeStatus.CAPTURED_PENDING_DQ:
        raise BackfillStateError("DQ may complete only a captured scope")
    return replace(
        scope,
        status=ScopeStatus.COMPLETE,
        scope_manifest_uri=scope_manifest_uri,
        scope_manifest_sha256=scope_manifest_sha256,
        raw_evidence_ids=_normalise_raw_evidence(
            (*scope.raw_evidence_ids, *tuple(raw_evidence_ids))
        ),
        last_error_class=None,
        last_error_message=None,
        updated_at=_utc_datetime("now", now),
        revision=scope.revision + 1,
    )


@dataclass(frozen=True)
class CompletionAccounting:
    target_total: int
    pending: int
    running: int
    captured_pending_dq: int
    retryable_error: int
    complete: int
    unavailable: int
    terminal_error: int

    @property
    def terminal_total(self) -> int:
        return self.complete + self.unavailable + self.terminal_error

    @property
    def open_total(self) -> int:
        return self.target_total - self.terminal_total

    @property
    def is_complete(self) -> bool:
        return self.target_total == self.terminal_total and self.open_total == 0

    def as_dict(self) -> dict[str, int | bool]:
        payload = asdict(self)
        payload.update(
            terminal_total=self.terminal_total,
            open_total=self.open_total,
            is_complete=self.is_complete,
        )
        return payload


def accounting_for(
    campaign: BackfillCampaign,
    scopes: Iterable[BackfillScopeState],
) -> CompletionAccounting:
    """Partition the exact frozen denominator; missing/extra rows are errors."""

    items = tuple(scopes)
    scope_ids = [item.target.scope_id for item in items]
    if len(scope_ids) != len(set(scope_ids)):
        raise BackfillStateError("scope state contains duplicate scope_id")
    expected = {item.scope_id for item in campaign.targets}
    actual = set(scope_ids)
    if actual != expected:
        raise BackfillStateError(
            "scope state does not exactly equal frozen target: "
            f"missing={sorted(expected - actual)} extra={sorted(actual - expected)}"
        )
    for item in items:
        if item.campaign_id != campaign.campaign_id:
            raise BackfillStateError("scope state belongs to another campaign")
        if item.target not in campaign.targets:
            raise BackfillStateError("scope state target differs from frozen target")
    counts = {status: 0 for status in ScopeStatus}
    for item in items:
        counts[item.status] += 1
    result = CompletionAccounting(
        target_total=len(expected),
        pending=counts[ScopeStatus.PENDING],
        running=counts[ScopeStatus.RUNNING],
        captured_pending_dq=counts[ScopeStatus.CAPTURED_PENDING_DQ],
        retryable_error=counts[ScopeStatus.RETRYABLE_ERROR],
        complete=counts[ScopeStatus.COMPLETE],
        unavailable=counts[ScopeStatus.UNAVAILABLE],
        terminal_error=counts[ScopeStatus.TERMINAL_ERROR],
    )
    if sum(
        (
            result.pending,
            result.running,
            result.captured_pending_dq,
            result.retryable_error,
            result.complete,
            result.unavailable,
            result.terminal_error,
        )
    ) != result.target_total:
        raise BackfillStateError("completion accounting is not an exact partition")
    return result


@dataclass(frozen=True)
class CanonicalArtifact:
    payload: Mapping[str, Any]
    canonical_json: str
    sha256: str


@dataclass(frozen=True)
class CompletionEvidence:
    attempt_count: int
    batch_count: int
    attempt_relation_sha256: str
    batch_relation_sha256: str
    raw_evidence_sha256: str


def verify_completion_evidence(
    campaign: BackfillCampaign,
    scopes: Iterable[BackfillScopeState],
    attempts: Iterable[BackfillAttempt],
    batches: Iterable[BackfillBatch],
) -> CompletionEvidence:
    """Prove the terminal scopes from exact attempts and completed DQ batches."""

    scope_items = tuple(scopes)
    accounting = accounting_for(campaign, scope_items)
    if not accounting.is_complete:
        raise BackfillStateError("completion evidence has non-terminal scopes")
    scope_by_id = {item.target.scope_id: item for item in scope_items}

    attempt_items = tuple(attempts)
    attempt_ids = [item.attempt_id for item in attempt_items]
    if len(attempt_ids) != len(set(attempt_ids)):
        raise BackfillStateError("completion evidence has duplicate attempt_id")
    by_scope: dict[str, list[BackfillAttempt]] = {
        scope_id: [] for scope_id in scope_by_id
    }
    for attempt in attempt_items:
        if attempt.campaign_id != campaign.campaign_id:
            raise BackfillStateError("completion attempt belongs to another campaign")
        if attempt.scope_id not in by_scope:
            raise BackfillStateError("completion attempt is outside frozen target")
        by_scope[attempt.scope_id].append(attempt)

    batch_items = tuple(batches)
    batch_ids = [item.batch_id for item in batch_items]
    if len(batch_ids) != len(set(batch_ids)):
        raise BackfillStateError("completion evidence has duplicate batch_id")
    batch_by_id = {item.batch_id: item for item in batch_items}
    for batch in batch_items:
        if batch.campaign_id != campaign.campaign_id:
            raise BackfillStateError("completion batch belongs to another campaign")
        if batch.status is not BatchStatus.COMPLETE:
            raise BackfillStateError("completion evidence contains incomplete batch")
        if batch.open_platform_incident_id is not None:
            raise BackfillStateError(
                "completion evidence contains an open platform incident"
            )
        if not set(batch.scope_ids) <= set(scope_by_id):
            raise BackfillStateError("completion batch is outside frozen target")

    for scope_id, scope in scope_by_id.items():
        linked = sorted(by_scope[scope_id], key=lambda item: item.sequence)
        if [item.sequence for item in linked] != list(
            range(1, scope.attempt_count + 1)
        ):
            raise BackfillStateError(
                f"{scope_id}: attempt rows do not exactly match attempt_count"
            )
        source_attempts = [
            item for item in linked
            if item.outcome is not AttemptOutcome.PLATFORM_ERROR
        ]
        source_errors = [
            item for item in linked
            if item.outcome is AttemptOutcome.SOURCE_ERROR
        ]
        if len(source_attempts) != scope.source_attempt_count:
            raise BackfillStateError(
                f"{scope_id}: source_attempt_count differs from attempts"
            )
        if len(source_errors) != scope.source_error_count:
            raise BackfillStateError(
                f"{scope_id}: source_error_count differs from attempts"
            )
        confirmation_days = tuple(sorted({
            item.source_observed_at.date()
            for item in linked
            if item.outcome is AttemptOutcome.UNAVAILABLE_CONFIRMATION
            and item.source_observed_at is not None
        }))
        if confirmation_days != scope.unavailable_confirmation_days:
            raise BackfillStateError(
                f"{scope_id}: unavailable days differ from immutable source evidence"
            )
        if not linked:
            raise BackfillStateError(f"{scope_id}: terminal scope has no attempts")
        last = linked[-1]
        expected_outcome = {
            ScopeStatus.COMPLETE: AttemptOutcome.CAPTURED,
            ScopeStatus.UNAVAILABLE: AttemptOutcome.UNAVAILABLE_CONFIRMATION,
            ScopeStatus.TERMINAL_ERROR: AttemptOutcome.SOURCE_ERROR,
        }[scope.status]
        if last.outcome is not expected_outcome:
            raise BackfillStateError(
                f"{scope_id}: terminal status is not bound to final attempt"
            )
        attempt_raw = {
            capture_id for item in linked for capture_id in item.raw_evidence_ids
        }
        if not attempt_raw <= set(scope.raw_evidence_ids):
            raise BackfillStateError(
                f"{scope_id}: scope raw evidence omits attempt evidence"
            )
        for attempt in linked:
            batch = batch_by_id.get(attempt.batch_id)
            if batch is None:
                raise BackfillStateError(
                    f"{scope_id}: attempt references missing batch"
                )
            if scope_id not in batch.scope_ids:
                raise BackfillStateError(
                    f"{scope_id}: attempt batch omits its scope"
                )
            position = batch.scope_ids.index(scope_id)
            if (
                batch.scope_claim_generations[position]
                != attempt.claim_generation
            ):
                raise BackfillStateError(
                    f"{scope_id}: attempt/batch claim generation mismatch"
                )

    attempts_by_batch: dict[str, list[BackfillAttempt]] = {
        batch_id: [] for batch_id in batch_by_id
    }
    for attempt in attempt_items:
        attempts_by_batch[attempt.batch_id].append(attempt)
    for batch_id, batch in batch_by_id.items():
        attempt_raw = {
            capture_id
            for item in attempts_by_batch[batch_id]
            for capture_id in item.raw_evidence_ids
        }
        if not attempt_raw <= set(batch.raw_evidence_ids):
            raise BackfillStateError(
                f"{batch_id}: batch raw evidence omits attempt evidence"
            )

    attempt_relation = [
        (item.attempt_id, record_sha256(item))
        for item in sorted(attempt_items, key=lambda item: item.attempt_id)
    ]
    batch_relation = [
        (item.batch_id, record_sha256(item))
        for item in sorted(batch_items, key=lambda item: item.batch_id)
    ]
    raw_ids = sorted({
        capture_id
        for scope in scope_items
        for capture_id in scope.raw_evidence_ids
    } | {
        capture_id
        for batch in batch_items
        for capture_id in batch.raw_evidence_ids
    })
    return CompletionEvidence(
        attempt_count=len(attempt_items),
        batch_count=len(batch_items),
        attempt_relation_sha256=canonical_sha256(attempt_relation),
        batch_relation_sha256=canonical_sha256(batch_relation),
        raw_evidence_sha256=canonical_sha256(raw_ids),
    )


def completion_report(
    campaign: BackfillCampaign,
    scopes: Iterable[BackfillScopeState],
    *,
    attempts: Iterable[BackfillAttempt] = (),
    batches: Iterable[BackfillBatch] = (),
    require_complete: bool = True,
) -> CanonicalArtifact:
    items = tuple(sorted(scopes, key=lambda item: item.target.scope_id))
    accounting = accounting_for(campaign, items)
    if require_complete and not accounting.is_complete:
        raise BackfillStateError("campaign still has non-terminal scopes")
    attempt_items = tuple(attempts)
    batch_items = tuple(batches)
    evidence = None
    if require_complete:
        evidence = verify_completion_evidence(
            campaign, items, attempt_items, batch_items
        )
    payload = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "campaign": {
            "campaign_id": campaign.campaign_id,
            "registry_snapshot_id": campaign.registry_snapshot_id,
            "policy_sha256": campaign.policy_sha256,
            "parser_revision": campaign.parser_revision,
            "schema_revision": campaign.schema_revision,
            "target_sha256": campaign.target_sha256,
        },
        "accounting": accounting.as_dict(),
        "evidence": asdict(evidence) if evidence is not None else None,
        "scopes": [
            {
                "scope_id": item.target.scope_id,
                "competition_id": item.target.competition_id,
                "edition_id": item.target.edition_id,
                "status": item.status.value,
                "attempt_count": item.attempt_count,
                "source_attempt_count": item.source_attempt_count,
                "source_error_count": item.source_error_count,
                "unavailable_confirmation_days": list(
                    item.unavailable_confirmation_days
                ),
                "checkpoint_uri": item.checkpoint_uri,
                "checkpoint_sha256": item.checkpoint_sha256,
                "scope_manifest_uri": item.scope_manifest_uri,
                "scope_manifest_sha256": item.scope_manifest_sha256,
                "raw_evidence_ids": list(item.raw_evidence_ids),
                "last_error_class": item.last_error_class,
            }
            for item in items
        ],
        "attempts": [
            {
                "attempt_id": item.attempt_id,
                "record_sha256": record_sha256(item),
                "batch_id": item.batch_id,
                "scope_id": item.scope_id,
                "sequence": item.sequence,
                "claim_generation": item.claim_generation,
                "outcome": item.outcome.value,
                "source_observed_at": item.source_observed_at,
                "raw_evidence_ids": list(item.raw_evidence_ids),
            }
            for item in sorted(attempt_items, key=lambda item: item.attempt_id)
        ],
        "batches": [
            {
                "batch_id": item.batch_id,
                "record_sha256": record_sha256(item),
                "scope_ids": list(item.scope_ids),
                "scope_claim_generations": list(item.scope_claim_generations),
                "status": item.status.value,
                "snapshot_pins": item.snapshot_pins,
                "dq_report_uri": item.dq_report_uri,
                "dq_report_sha256": item.dq_report_sha256,
                "raw_evidence_ids": list(item.raw_evidence_ids),
                "platform_incidents": [
                    {
                        "incident_id": incident.incident_id,
                        "phase": incident.phase,
                        "error_class": incident.error_class,
                        "blocked_from_status": (
                            incident.blocked_from_status.value
                        ),
                        "pre_incident_batch_revision": (
                            incident.pre_incident_batch_revision
                        ),
                        "report_uri": incident.report_uri,
                        "report_sha256": incident.report_sha256,
                        "raw_evidence_ids": list(incident.raw_evidence_ids),
                        "created_at": incident.created_at,
                    }
                    for incident in item.platform_incidents
                ],
                "open_platform_incident_id": (
                    item.open_platform_incident_id
                ),
            }
            for item in sorted(batch_items, key=lambda item: item.batch_id)
        ],
    }
    payload = _normalise_json(payload)
    assert isinstance(payload, dict)
    rendered = canonical_json(payload)
    return CanonicalArtifact(
        payload=payload,
        canonical_json=rendered,
        sha256=hashlib.sha256(rendered.encode("utf-8")).hexdigest(),
    )


def complete_campaign(
    campaign: BackfillCampaign,
    scopes: Iterable[BackfillScopeState],
    *,
    attempts: Iterable[BackfillAttempt],
    batches: Iterable[BackfillBatch],
    report_uri: str,
    now: datetime,
) -> tuple[BackfillCampaign, CanonicalArtifact]:
    artifact = completion_report(
        campaign,
        scopes,
        attempts=attempts,
        batches=batches,
        require_complete=True,
    )
    completed = campaign.transition(
        CampaignStatus.COMPLETE,
        now=now,
        report_uri=_required_text("report_uri", report_uri),
        report_sha256=artifact.sha256,
    )
    return completed, artifact


def verify_completion_report(
    campaign: BackfillCampaign,
    scopes: Iterable[BackfillScopeState],
    attempts: Iterable[BackfillAttempt],
    batches: Iterable[BackfillBatch],
    artifact: CanonicalArtifact,
) -> None:
    expected = completion_report(
        campaign,
        scopes,
        attempts=attempts,
        batches=batches,
        require_complete=True,
    )
    if artifact != expected:
        raise BackfillStateError("completion report differs from exact evidence")
    if campaign.status is CampaignStatus.COMPLETE:
        if campaign.report_sha256 != artifact.sha256:
            raise BackfillStateError("campaign report hash differs from evidence")


def record_payload(record: Any) -> dict[str, Any]:
    if not is_dataclass(record) or isinstance(record, type):
        raise BackfillStateError("record must be a contract dataclass")
    payload = _normalise_json(record)
    if not isinstance(payload, dict):
        raise BackfillStateError("record payload must be an object")
    return payload


def campaign_storage_payload(campaign: BackfillCampaign) -> dict[str, Any]:
    """Compact campaign row; denominator bodies live in immutable scope rows."""

    payload = record_payload(campaign)
    payload.pop("targets", None)
    payload["target_count"] = len(campaign.targets)
    return payload


def record_sha256(record: Any) -> str:
    return canonical_sha256(record_payload(record))


def _parse_datetime(name: str, value: Any) -> datetime:
    if isinstance(value, datetime):
        return _utc_datetime(name, value)
    if not isinstance(value, str):
        raise BackfillStateError(f"{name} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BackfillStateError(
            f"{name} must be an ISO-8601 timestamp"
        ) from exc
    return _utc_datetime(name, parsed)


def _parse_optional_datetime(name: str, value: Any) -> datetime | None:
    if value is None:
        return None
    return _parse_datetime(name, value)


def _parse_record_payload(row: Mapping[str, Any]) -> Mapping[str, Any]:
    if not isinstance(row, Mapping):
        raise BackfillStateError("state row must be a mapping")
    raw = row.get("record_json")
    if not isinstance(raw, str) or not raw:
        raise BackfillStateError("state row lacks record_json")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BackfillStateError("state row record_json is invalid") from exc
    if not isinstance(payload, Mapping):
        raise BackfillStateError("state row record_json must be an object")
    if canonical_json(payload) != raw:
        raise BackfillStateError("state row record_json is not canonical")
    expected_hash = canonical_sha256(payload)
    if _required_sha256("record_sha256", row.get("record_sha256")) != expected_hash:
        raise BackfillStateError("state row record_sha256 mismatch")
    return payload


def _assert_projection(
    row: Mapping[str, Any], name: str, expected: Any
) -> None:
    if name not in row:
        return
    actual = row[name]
    if isinstance(expected, Enum):
        expected = expected.value
    if isinstance(expected, int) and not isinstance(expected, bool):
        try:
            actual = int(actual)
        except (TypeError, ValueError) as exc:
            raise BackfillStateError(f"invalid projected {name}") from exc
    elif expected is not None:
        actual = str(actual)
    if actual != expected:
        raise BackfillStateError(f"projected {name} differs from record_json")


def campaign_from_mapping(value: Mapping[str, Any]) -> BackfillCampaign:
    try:
        return BackfillCampaign(
            campaign_id=value["campaign_id"],
            registry_snapshot_id=value["registry_snapshot_id"],
            policy_sha256=value["policy_sha256"],
            parser_revision=value["parser_revision"],
            schema_revision=value["schema_revision"],
            targets=tuple(
                HistoricalScopeTarget.from_mapping(item)
                for item in value["targets"]
            ),
            target_sha256=value["target_sha256"],
            status=value["status"],
            created_at=_parse_datetime("created_at", value["created_at"]),
            updated_at=_parse_datetime("updated_at", value["updated_at"]),
            report_uri=value.get("report_uri"),
            report_sha256=value.get("report_sha256"),
            revision=int(value.get("revision", 0)),
            contract_version=int(value.get("contract_version", CONTRACT_VERSION)),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, BackfillStateError):
            raise
        raise BackfillStateError("campaign record is incomplete") from exc


def scope_from_mapping(value: Mapping[str, Any]) -> BackfillScopeState:
    try:
        days = tuple(date.fromisoformat(str(item)) for item in value[
            "unavailable_confirmation_days"
        ])
        return BackfillScopeState(
            campaign_id=value["campaign_id"],
            target=HistoricalScopeTarget.from_mapping(value["target"]),
            status=value["status"],
            attempt_count=int(value["attempt_count"]),
            source_attempt_count=int(value["source_attempt_count"]),
            source_error_count=int(value["source_error_count"]),
            claim_generation=int(value["claim_generation"]),
            unavailable_confirmation_days=days,
            next_retry_at=_parse_optional_datetime(
                "next_retry_at", value.get("next_retry_at")
            ),
            batch_id=value.get("batch_id"),
            lease_id=value.get("lease_id"),
            lease_owner=value.get("lease_owner"),
            leased_at=_parse_optional_datetime("leased_at", value.get("leased_at")),
            heartbeat_at=_parse_optional_datetime(
                "heartbeat_at", value.get("heartbeat_at")
            ),
            checkpoint_uri=value.get("checkpoint_uri"),
            checkpoint_sha256=value.get("checkpoint_sha256"),
            scope_manifest_uri=value.get("scope_manifest_uri"),
            scope_manifest_sha256=value.get("scope_manifest_sha256"),
            raw_evidence_ids=tuple(value.get("raw_evidence_ids", ())),
            last_error_class=value.get("last_error_class"),
            last_error_message=value.get("last_error_message"),
            updated_at=_parse_datetime("updated_at", value["updated_at"]),
            revision=int(value.get("revision", 0)),
            contract_version=int(value.get("contract_version", CONTRACT_VERSION)),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, BackfillStateError):
            raise
        raise BackfillStateError("scope record is incomplete") from exc


def attempt_from_mapping(value: Mapping[str, Any]) -> BackfillAttempt:
    try:
        retry_after = value.get("retry_after_seconds")
        return BackfillAttempt(
            attempt_id=value["attempt_id"],
            campaign_id=value["campaign_id"],
            batch_id=value["batch_id"],
            scope_id=value["scope_id"],
            sequence=int(value["sequence"]),
            claim_generation=int(value["claim_generation"]),
            outcome=value["outcome"],
            started_at=_parse_datetime("started_at", value["started_at"]),
            finished_at=_parse_datetime("finished_at", value["finished_at"]),
            raw_evidence_ids=tuple(value.get("raw_evidence_ids", ())),
            source_observed_at=_parse_optional_datetime(
                "source_observed_at", value.get("source_observed_at")
            ),
            error_class=value.get("error_class"),
            error_message=value.get("error_message"),
            retry_after_seconds=(
                None if retry_after is None else int(retry_after)
            ),
            checkpoint_uri=value.get("checkpoint_uri"),
            checkpoint_sha256=value.get("checkpoint_sha256"),
            scope_manifest_uri=value.get("scope_manifest_uri"),
            scope_manifest_sha256=value.get("scope_manifest_sha256"),
            contract_version=int(value.get("contract_version", CONTRACT_VERSION)),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, BackfillStateError):
            raise
        raise BackfillStateError("attempt record is incomplete") from exc


def platform_incident_from_mapping(
    value: Mapping[str, Any],
) -> BackfillPlatformIncident:
    try:
        return BackfillPlatformIncident(
            incident_id=value["incident_id"],
            campaign_id=value["campaign_id"],
            batch_id=value["batch_id"],
            phase=value["phase"],
            error_class=value["error_class"],
            blocked_from_status=value["blocked_from_status"],
            pre_incident_batch_revision=int(
                value["pre_incident_batch_revision"]
            ),
            report_uri=value["report_uri"],
            report_sha256=value["report_sha256"],
            raw_evidence_ids=tuple(value.get("raw_evidence_ids", ())),
            created_at=_parse_datetime("created_at", value["created_at"]),
            contract_version=int(value.get("contract_version", CONTRACT_VERSION)),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, BackfillStateError):
            raise
        raise BackfillStateError("platform incident record is incomplete") from exc


def batch_from_mapping(value: Mapping[str, Any]) -> BackfillBatch:
    try:
        return BackfillBatch(
            batch_id=value["batch_id"],
            campaign_id=value["campaign_id"],
            scope_ids=tuple(value["scope_ids"]),
            scope_claim_generations=tuple(value["scope_claim_generations"]),
            status=value["status"],
            claimed_at=_parse_datetime("claimed_at", value["claimed_at"]),
            updated_at=_parse_datetime("updated_at", value["updated_at"]),
            completed_at=_parse_optional_datetime(
                "completed_at", value.get("completed_at")
            ),
            snapshot_pins=value.get("snapshot_pins"),
            dq_report_uri=value.get("dq_report_uri"),
            dq_report_sha256=value.get("dq_report_sha256"),
            raw_evidence_ids=tuple(value.get("raw_evidence_ids", ())),
            platform_incidents=tuple(
                platform_incident_from_mapping(item)
                for item in value.get("platform_incidents", ())
            ),
            open_platform_incident_id=value.get(
                "open_platform_incident_id"
            ),
            revision=int(value.get("revision", 0)),
            contract_version=int(value.get("contract_version", CONTRACT_VERSION)),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, BackfillStateError):
            raise
        raise BackfillStateError("batch record is incomplete") from exc


def parse_campaign_row(
    row: Mapping[str, Any],
    *,
    targets: Iterable[HistoricalScopeTarget] | None = None,
) -> BackfillCampaign:
    payload = dict(_parse_record_payload(row))
    supplied = tuple(targets or ())
    if "targets" not in payload:
        if not supplied:
            raise BackfillStateError(
                "compact campaign row requires its exact denominator rows"
            )
        try:
            declared_count = int(payload.pop("target_count"))
        except (KeyError, TypeError, ValueError) as exc:
            raise BackfillStateError(
                "compact campaign target_count is invalid"
            ) from exc
        if declared_count != len(supplied):
            raise BackfillStateError("compact campaign denominator is incomplete")
        payload["targets"] = [item.identity_payload() for item in supplied]
    record = campaign_from_mapping(payload)
    if supplied and tuple(record.targets) != freeze_historical_targets(
        supplied,
        registry_snapshot_id=record.registry_snapshot_id,
    ):
        raise BackfillStateError("campaign denominator rows differ from record")
    for name, expected in (
        ("campaign_id", record.campaign_id),
        ("target_sha256", record.target_sha256),
        ("target_count", len(record.targets)),
        ("status", record.status),
        ("revision", record.revision),
    ):
        _assert_projection(row, name, expected)
    return record


def parse_campaign_pointer_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and expose a compact campaign header before denominator fill."""

    payload = dict(_parse_record_payload(row))
    if "targets" in payload:
        raise BackfillStateError("campaign pointer row is not compact")
    try:
        target_count = int(payload["target_count"])
        status = CampaignStatus(payload["status"])
    except (KeyError, TypeError, ValueError) as exc:
        raise BackfillStateError("campaign pointer row is incomplete") from exc
    if target_count <= 0:
        raise BackfillStateError("campaign pointer target_count must be positive")
    for field in (
        "campaign_id",
        "registry_snapshot_id",
        "policy_sha256",
        "parser_revision",
        "schema_revision",
        "target_sha256",
        "created_at",
        "updated_at",
    ):
        if field not in payload:
            raise BackfillStateError("campaign pointer row is incomplete")
    _required_sha256("campaign_id", payload["campaign_id"])
    _required_sha256("policy_sha256", payload["policy_sha256"])
    _required_sha256("target_sha256", payload["target_sha256"])
    _parse_datetime("created_at", payload["created_at"])
    _parse_datetime("updated_at", payload["updated_at"])
    for name, expected in (
        ("campaign_id", payload["campaign_id"]),
        ("target_sha256", payload["target_sha256"]),
        ("target_count", target_count),
        ("status", status),
        ("revision", int(payload.get("revision", 0))),
    ):
        _assert_projection(row, name, expected)
    return payload


def parse_scope_row(row: Mapping[str, Any]) -> BackfillScopeState:
    record = scope_from_mapping(_parse_record_payload(row))
    for name, expected in (
        ("campaign_id", record.campaign_id),
        ("scope_id", record.target.scope_id),
        ("status", record.status),
        ("revision", record.revision),
    ):
        _assert_projection(row, name, expected)
    return record


def parse_attempt_row(row: Mapping[str, Any]) -> BackfillAttempt:
    record = attempt_from_mapping(_parse_record_payload(row))
    for name, expected in (
        ("attempt_id", record.attempt_id),
        ("campaign_id", record.campaign_id),
        ("scope_id", record.scope_id),
        ("sequence", record.sequence),
        ("claim_generation", record.claim_generation),
        ("outcome", record.outcome),
    ):
        _assert_projection(row, name, expected)
    return record


def parse_batch_row(row: Mapping[str, Any]) -> BackfillBatch:
    record = batch_from_mapping(_parse_record_payload(row))
    for name, expected in (
        ("batch_id", record.batch_id),
        ("campaign_id", record.campaign_id),
        ("status", record.status),
        ("revision", record.revision),
    ):
        _assert_projection(row, name, expected)
    return record


def ddl_statements() -> tuple[str, ...]:
    """Return additive Iceberg/Trino DDL; this function never executes it."""

    return (
        f"""CREATE TABLE IF NOT EXISTS {CAMPAIGN_TABLE} (
    campaign_id varchar, contract_version bigint, registry_snapshot_id varchar,
    policy_sha256 varchar, parser_revision varchar, schema_revision varchar,
    target_sha256 varchar, target_count bigint, status varchar,
    report_uri varchar, report_sha256 varchar, record_json varchar,
    record_sha256 varchar, revision bigint, created_at timestamp(6),
    updated_at timestamp(6)
) WITH (format = 'PARQUET')""",
        f"""CREATE TABLE IF NOT EXISTS {SCOPE_TABLE} (
    campaign_id varchar, scope_id varchar, status varchar, batch_id varchar,
    attempt_count bigint, source_attempt_count bigint,
    source_error_count bigint, claim_generation bigint,
    next_retry_at timestamp(6),
    lease_id varchar, lease_owner varchar, leased_at timestamp(6),
    heartbeat_at timestamp(6), checkpoint_uri varchar,
    checkpoint_sha256 varchar, scope_manifest_uri varchar,
    scope_manifest_sha256 varchar, raw_evidence_ids_json varchar,
    target_json varchar, record_json varchar, record_sha256 varchar,
    revision bigint, updated_at timestamp(6)
) WITH (format = 'PARQUET')""",
        f"""CREATE TABLE IF NOT EXISTS {ATTEMPT_TABLE} (
    attempt_id varchar, campaign_id varchar, batch_id varchar, scope_id varchar,
    sequence bigint, claim_generation bigint, outcome varchar,
    started_at timestamp(6),
    finished_at timestamp(6), raw_evidence_ids_json varchar,
    source_observed_at timestamp(6),
    checkpoint_uri varchar, checkpoint_sha256 varchar,
    scope_manifest_uri varchar, scope_manifest_sha256 varchar,
    record_json varchar, record_sha256 varchar
) WITH (format = 'PARQUET')""",
        f"""CREATE TABLE IF NOT EXISTS {BATCH_TABLE} (
    batch_id varchar, campaign_id varchar, scope_ids_json varchar,
    scope_claim_generations_json varchar, status varchar,
    claimed_at timestamp(6), completed_at timestamp(6),
    snapshot_pins_json varchar, dq_report_uri varchar,
    dq_report_sha256 varchar, raw_evidence_ids_json varchar,
    record_json varchar, record_sha256 varchar, revision bigint,
    updated_at timestamp(6)
) WITH (format = 'PARQUET')""",
    )


def _sql_literal(value: Any, sql_type: str) -> str:
    if value is None:
        return f"CAST(NULL AS {sql_type})"
    if sql_type == "bigint":
        if isinstance(value, bool) or not isinstance(value, int):
            raise BackfillStateError("SQL bigint value must be an integer")
        return f"BIGINT '{value}'"
    if sql_type == "timestamp(6)":
        parsed = _utc_datetime("SQL timestamp", value)
        return "TIMESTAMP '" + parsed.strftime("%Y-%m-%d %H:%M:%S.%f") + "'"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _cas_merge_sql(
    *,
    table: str,
    key_columns: Sequence[str],
    columns: Mapping[str, tuple[Any, str]],
    expected_revision: int,
) -> str:
    if isinstance(expected_revision, bool) or expected_revision < -1:
        raise BackfillStateError("expected_revision must be -1 or non-negative")
    revision_value = columns.get("revision", (0, "bigint"))[0]
    if revision_value != expected_revision + 1:
        raise BackfillStateError(
            "record revision must be exactly expected_revision + 1"
        )
    names = tuple(columns)
    values = ", ".join(
        _sql_literal(value, sql_type)
        for value, sql_type in columns.values()
    )
    source_names = ", ".join((*names, "expected_revision"))
    source_values = values + ", " + _sql_literal(expected_revision, "bigint")
    predicate = " AND ".join(
        f"target.{name} = source.{name}" for name in key_columns
    )
    mutable = [name for name in names if name not in key_columns]
    updates = ",\n    ".join(
        f"{name} = source.{name}" for name in mutable
    )
    inserts = ", ".join(names)
    insert_values = ", ".join(f"source.{name}" for name in names)
    return f"""MERGE INTO {table} target
USING (VALUES ({source_values})) source ({source_names})
ON {predicate}
WHEN MATCHED AND target.revision = source.expected_revision THEN UPDATE SET
    {updates}
WHEN NOT MATCHED AND source.expected_revision = BIGINT '-1' THEN INSERT (
    {inserts}
) VALUES (
    {insert_values}
)"""


def _immutable_merge_sql(
    *,
    table: str,
    key_columns: Sequence[str],
    columns: Mapping[str, tuple[Any, str]],
) -> str:
    """Render an idempotent insert for append-only, hash-bound records.

    A repeated identical row performs a harmless no-op update.  A conflicting
    row matches the key but not the hash and is left untouched; the caller must
    verify the persisted hash with :func:`record_readback_sql`.
    """

    if "record_sha256" not in columns:
        raise BackfillStateError("immutable MERGE requires record_sha256")
    names = tuple(columns)
    values = ", ".join(
        _sql_literal(value, sql_type)
        for value, sql_type in columns.values()
    )
    source_names = ", ".join(names)
    predicate = " AND ".join(
        f"target.{name} = source.{name}" for name in key_columns
    )
    inserts = ", ".join(names)
    insert_values = ", ".join(f"source.{name}" for name in names)
    return f"""MERGE INTO {table} target
USING (VALUES ({values})) source ({source_names})
ON {predicate}
WHEN MATCHED AND target.record_sha256 = source.record_sha256 THEN UPDATE SET
    record_sha256 = target.record_sha256
WHEN NOT MATCHED THEN INSERT (
    {inserts}
) VALUES (
    {insert_values}
)"""


def campaign_merge_sql(
    campaign: BackfillCampaign, *, expected_revision: int | None = None
) -> str:
    payload = campaign_storage_payload(campaign)
    expected = campaign.revision - 1 if expected_revision is None else expected_revision
    columns = {
        "campaign_id": (campaign.campaign_id, "varchar"),
        "contract_version": (campaign.contract_version, "bigint"),
        "registry_snapshot_id": (campaign.registry_snapshot_id, "varchar"),
        "policy_sha256": (campaign.policy_sha256, "varchar"),
        "parser_revision": (campaign.parser_revision, "varchar"),
        "schema_revision": (campaign.schema_revision, "varchar"),
        "target_sha256": (campaign.target_sha256, "varchar"),
        "target_count": (len(campaign.targets), "bigint"),
        "status": (campaign.status.value, "varchar"),
        "report_uri": (campaign.report_uri, "varchar"),
        "report_sha256": (campaign.report_sha256, "varchar"),
        "record_json": (canonical_json(payload), "varchar"),
        "record_sha256": (canonical_sha256(payload), "varchar"),
        "revision": (campaign.revision, "bigint"),
        "created_at": (campaign.created_at, "timestamp(6)"),
        "updated_at": (campaign.updated_at, "timestamp(6)"),
    }
    return _cas_merge_sql(
        table=CAMPAIGN_TABLE,
        key_columns=("campaign_id",),
        columns=columns,
        expected_revision=expected,
    )


def _scope_columns(
    scope: BackfillScopeState,
) -> dict[str, tuple[Any, str]]:
    payload = record_payload(scope)
    return {
        "campaign_id": (scope.campaign_id, "varchar"),
        "scope_id": (scope.target.scope_id, "varchar"),
        "status": (scope.status.value, "varchar"),
        "batch_id": (scope.batch_id, "varchar"),
        "attempt_count": (scope.attempt_count, "bigint"),
        "source_attempt_count": (scope.source_attempt_count, "bigint"),
        "source_error_count": (scope.source_error_count, "bigint"),
        "claim_generation": (scope.claim_generation, "bigint"),
        "next_retry_at": (scope.next_retry_at, "timestamp(6)"),
        "lease_id": (scope.lease_id, "varchar"),
        "lease_owner": (scope.lease_owner, "varchar"),
        "leased_at": (scope.leased_at, "timestamp(6)"),
        "heartbeat_at": (scope.heartbeat_at, "timestamp(6)"),
        "checkpoint_uri": (scope.checkpoint_uri, "varchar"),
        "checkpoint_sha256": (scope.checkpoint_sha256, "varchar"),
        "scope_manifest_uri": (scope.scope_manifest_uri, "varchar"),
        "scope_manifest_sha256": (scope.scope_manifest_sha256, "varchar"),
        "raw_evidence_ids_json": (
            canonical_json(scope.raw_evidence_ids), "varchar"
        ),
        "target_json": (canonical_json(scope.target), "varchar"),
        "record_json": (canonical_json(payload), "varchar"),
        "record_sha256": (canonical_sha256(payload), "varchar"),
        "revision": (scope.revision, "bigint"),
        "updated_at": (scope.updated_at, "timestamp(6)"),
    }


def scope_merge_sql(
    scope: BackfillScopeState, *, expected_revision: int | None = None
) -> str:
    expected = scope.revision - 1 if expected_revision is None else expected_revision
    return _cas_merge_sql(
        table=SCOPE_TABLE,
        key_columns=("campaign_id", "scope_id"),
        columns=_scope_columns(scope),
        expected_revision=expected,
    )


def initial_scope_chunk_merge_sql(
    scopes: Iterable[BackfillScopeState],
) -> str:
    """Install a bounded chunk of immutable revision-zero denominator rows."""

    items = tuple(scopes)
    if not items or len(items) > 256:
        raise BackfillStateError("initial scope chunk must contain 1..256 rows")
    campaign_ids = {item.campaign_id for item in items}
    scope_ids = {item.target.scope_id for item in items}
    if len(campaign_ids) != 1 or len(scope_ids) != len(items):
        raise BackfillStateError("initial scope chunk identity is invalid")
    if any(item.revision != 0 for item in items):
        raise BackfillStateError("initial scope chunk requires revision zero")

    column_sets = tuple(_scope_columns(item) for item in items)
    names = tuple(column_sets[0])
    if any(tuple(columns) != names for columns in column_sets):
        raise BackfillStateError("initial scope chunk columns differ")
    rows = ",\n    ".join(
        "(" + ", ".join(
            _sql_literal(value, sql_type)
            for value, sql_type in columns.values()
        ) + ", BIGINT '-1')"
        for columns in column_sets
    )
    source_names = ", ".join((*names, "expected_revision"))
    predicate = (
        "target.campaign_id = source.campaign_id "
        "AND target.scope_id = source.scope_id"
    )
    mutable = [name for name in names if name not in {"campaign_id", "scope_id"}]
    updates = ",\n    ".join(
        f"{name} = source.{name}" for name in mutable
    )
    inserts = ", ".join(names)
    insert_values = ", ".join(f"source.{name}" for name in names)
    return f"""MERGE INTO {SCOPE_TABLE} target
USING (VALUES
    {rows}
) source ({source_names})
ON {predicate}
WHEN MATCHED AND target.revision = source.expected_revision THEN UPDATE SET
    {updates}
WHEN NOT MATCHED AND source.expected_revision = BIGINT '-1' THEN INSERT (
    {inserts}
) VALUES (
    {insert_values}
)"""


def initial_scope_chunk_readback_sql(
    scopes: Iterable[BackfillScopeState],
) -> str:
    items = tuple(scopes)
    if not items or len(items) > 256:
        raise BackfillStateError("initial scope chunk must contain 1..256 rows")
    campaign_ids = {item.campaign_id for item in items}
    scope_ids = {item.target.scope_id for item in items}
    if len(campaign_ids) != 1 or len(scope_ids) != len(items):
        raise BackfillStateError("initial scope chunk identity is invalid")
    campaign_id = next(iter(campaign_ids)).replace("'", "''")
    identities = ", ".join(
        _sql_literal(scope_id, "varchar") for scope_id in sorted(scope_ids)
    )
    return (
        f"SELECT scope_id, record_sha256, revision FROM {SCOPE_TABLE} "
        f"WHERE campaign_id = '{campaign_id}' AND scope_id IN ({identities})"
    )


def verify_initial_scope_chunk_readback(
    scopes: Iterable[BackfillScopeState],
    rows: Iterable[Mapping[str, Any]],
) -> None:
    items = tuple(scopes)
    expected = {
        item.target.scope_id: (record_sha256(item), item.revision)
        for item in items
    }
    values = tuple(rows)
    if len(values) != len(expected):
        raise BackfillStateError("initial scope chunk readback cardinality mismatch")
    actual: dict[str, tuple[str, int]] = {}
    for row in values:
        scope_id = str(row.get("scope_id") or "")
        if scope_id not in expected or scope_id in actual:
            raise BackfillStateError("initial scope chunk readback identity mismatch")
        try:
            revision = int(row["revision"])
        except (KeyError, TypeError, ValueError) as exc:
            raise BackfillStateError(
                "initial scope chunk readback revision is invalid"
            ) from exc
        actual[scope_id] = (
            _required_sha256("record_sha256", row.get("record_sha256")),
            revision,
        )
    if actual != expected:
        raise BackfillStateError("initial scope chunk readback differs")


def attempt_merge_sql(attempt: BackfillAttempt) -> str:
    payload = record_payload(attempt)
    columns = {
        "attempt_id": (attempt.attempt_id, "varchar"),
        "campaign_id": (attempt.campaign_id, "varchar"),
        "batch_id": (attempt.batch_id, "varchar"),
        "scope_id": (attempt.scope_id, "varchar"),
        "sequence": (attempt.sequence, "bigint"),
        "claim_generation": (attempt.claim_generation, "bigint"),
        "outcome": (attempt.outcome.value, "varchar"),
        "started_at": (attempt.started_at, "timestamp(6)"),
        "finished_at": (attempt.finished_at, "timestamp(6)"),
        "source_observed_at": (
            attempt.source_observed_at, "timestamp(6)"
        ),
        "raw_evidence_ids_json": (
            canonical_json(attempt.raw_evidence_ids), "varchar"
        ),
        "checkpoint_uri": (attempt.checkpoint_uri, "varchar"),
        "checkpoint_sha256": (attempt.checkpoint_sha256, "varchar"),
        "scope_manifest_uri": (attempt.scope_manifest_uri, "varchar"),
        "scope_manifest_sha256": (attempt.scope_manifest_sha256, "varchar"),
        "record_json": (canonical_json(payload), "varchar"),
        "record_sha256": (canonical_sha256(payload), "varchar"),
    }
    return _immutable_merge_sql(
        table=ATTEMPT_TABLE,
        key_columns=("attempt_id",),
        columns=columns,
    )


def batch_merge_sql(
    batch: BackfillBatch, *, expected_revision: int | None = None
) -> str:
    payload = record_payload(batch)
    expected = batch.revision - 1 if expected_revision is None else expected_revision
    columns = {
        "batch_id": (batch.batch_id, "varchar"),
        "campaign_id": (batch.campaign_id, "varchar"),
        "scope_ids_json": (canonical_json(batch.scope_ids), "varchar"),
        "scope_claim_generations_json": (
            canonical_json(batch.scope_claim_generations), "varchar"
        ),
        "status": (batch.status.value, "varchar"),
        "claimed_at": (batch.claimed_at, "timestamp(6)"),
        "completed_at": (batch.completed_at, "timestamp(6)"),
        "snapshot_pins_json": (
            canonical_json(batch.snapshot_pins or {}), "varchar"
        ),
        "dq_report_uri": (batch.dq_report_uri, "varchar"),
        "dq_report_sha256": (batch.dq_report_sha256, "varchar"),
        "raw_evidence_ids_json": (
            canonical_json(batch.raw_evidence_ids), "varchar"
        ),
        "record_json": (canonical_json(payload), "varchar"),
        "record_sha256": (canonical_sha256(payload), "varchar"),
        "revision": (batch.revision, "bigint"),
        "updated_at": (batch.updated_at, "timestamp(6)"),
    }
    return _cas_merge_sql(
        table=BATCH_TABLE,
        key_columns=("batch_id",),
        columns=columns,
        expected_revision=expected,
    )


def campaign_transition_merge_sql(
    previous: BackfillCampaign, current: BackfillCampaign
) -> str:
    if previous.campaign_id != current.campaign_id:
        raise BackfillStateError("campaign transition changes identity")
    if current.revision != previous.revision + 1:
        raise BackfillStateError("campaign transition must advance one revision")
    return campaign_merge_sql(current, expected_revision=previous.revision)


def scope_transition_merge_sql(
    previous: BackfillScopeState, current: BackfillScopeState
) -> str:
    if (
        previous.campaign_id,
        previous.target.scope_id,
    ) != (
        current.campaign_id,
        current.target.scope_id,
    ):
        raise BackfillStateError("scope transition changes identity")
    if current.target != previous.target:
        raise BackfillStateError("scope transition changes frozen target")
    if current.revision != previous.revision + 1:
        raise BackfillStateError("scope transition must advance one revision")
    return scope_merge_sql(current, expected_revision=previous.revision)


def batch_transition_merge_sql(
    previous: BackfillBatch, current: BackfillBatch
) -> str:
    if previous.batch_id != current.batch_id:
        raise BackfillStateError("batch transition changes identity")
    if current.scope_ids != previous.scope_ids:
        raise BackfillStateError("batch transition changes scope membership")
    if current.scope_claim_generations != previous.scope_claim_generations:
        raise BackfillStateError("batch transition changes claim generations")
    if current.revision != previous.revision + 1:
        raise BackfillStateError("batch transition must advance one revision")
    return batch_merge_sql(current, expected_revision=previous.revision)


def claim_merge_statements(
    previous_scopes: Iterable[BackfillScopeState],
    claim: ClaimResult,
) -> tuple[str, ...]:
    """Render the exact insert/CAS set produced by :func:`claim_scopes`."""

    before_items = tuple(previous_scopes)
    before = {item.target.scope_id: item for item in before_items}
    after = {item.target.scope_id: item for item in claim.scopes}
    if len(before) != len(before_items) or len(after) != len(claim.scopes):
        raise BackfillStateError("claim mutation contains duplicate scope_id")
    if set(before) != set(after):
        raise BackfillStateError("claim mutation changes the frozen scope set")
    statements: list[str] = []
    reclaimed = {
        item.target.scope_id: item for item in claim.reclaimed_scopes
    }
    if len(reclaimed) != len(claim.reclaimed_scopes):
        raise BackfillStateError("claim mutation has duplicate reclaimed scope")
    if set(reclaimed) != set(claim.reclaimed_scope_ids):
        raise BackfillStateError("reclaimed scope records do not match their IDs")
    for scope_id in sorted(reclaimed):
        if scope_id not in before:
            raise BackfillStateError("reclaimed scope is outside the frozen set")
        expected = reclaim_stale_lease(
            before[scope_id], now=reclaimed[scope_id].updated_at
        )
        if expected != reclaimed[scope_id]:
            raise BackfillStateError("reclaimed scope is not the exact transition")
        statements.append(scope_transition_merge_sql(
            before[scope_id], reclaimed[scope_id]
        ))
    if claim.batch is not None:
        statements.append(batch_merge_sql(claim.batch, expected_revision=-1))
    for scope_id in sorted(before):
        previous = reclaimed.get(scope_id, before[scope_id])
        current = after[scope_id]
        if record_sha256(previous) == record_sha256(current):
            continue
        statements.append(scope_transition_merge_sql(previous, current))
    return tuple(statements)


@dataclass(frozen=True)
class AttemptTransitionPlan:
    scope: BackfillScopeState
    campaign: BackfillCampaign
    statements: tuple[str, ...]


def plan_attempt_transition(
    campaign: BackfillCampaign,
    previous: BackfillScopeState,
    attempt: BackfillAttempt,
) -> AttemptTransitionPlan:
    if campaign.campaign_id != previous.campaign_id:
        raise BackfillStateError("scope belongs to another campaign")
    current = apply_attempt(previous, attempt)
    statements: list[str] = [
        attempt_merge_sql(attempt),
        scope_transition_merge_sql(previous, current),
    ]
    next_campaign = campaign
    if attempt_blocks_campaign(attempt):
        if campaign.status not in {
            CampaignStatus.ACTIVE,
            CampaignStatus.BLOCKED_PLATFORM,
        }:
            raise BackfillStateError(
                "platform error requires an active or already-blocked campaign"
            )
        if campaign.status is CampaignStatus.ACTIVE:
            next_campaign = campaign.transition(
                CampaignStatus.BLOCKED_PLATFORM,
                now=attempt.finished_at,
            )
            statements.append(campaign_transition_merge_sql(
                campaign, next_campaign
            ))
    return AttemptTransitionPlan(
        scope=current,
        campaign=next_campaign,
        statements=tuple(statements),
    )


def attempt_transition_merge_statements(
    previous: BackfillScopeState,
    attempt: BackfillAttempt,
    current: BackfillScopeState,
    *,
    campaign: BackfillCampaign | None = None,
) -> tuple[str, ...]:
    expected = apply_attempt(previous, attempt)
    if expected != current:
        raise BackfillStateError("scope state is not the exact attempt transition")
    if attempt_blocks_campaign(attempt):
        if campaign is None:
            raise BackfillStateError(
                "platform attempt transition requires campaign blocking CAS"
            )
        return plan_attempt_transition(campaign, previous, attempt).statements
    return (attempt_merge_sql(attempt), scope_transition_merge_sql(previous, current))


def campaign_select_sql(campaign_id: str) -> str:
    identity = _required_sha256("campaign_id", campaign_id).replace("'", "''")
    return (
        f"SELECT * FROM {CAMPAIGN_TABLE} "
        f"WHERE campaign_id = '{identity}'"
    )


def scope_select_sql(campaign_id: str) -> str:
    identity = _required_sha256("campaign_id", campaign_id).replace("'", "''")
    return (
        f"SELECT * FROM {SCOPE_TABLE} "
        f"WHERE campaign_id = '{identity}' ORDER BY scope_id"
    )


def record_readback_sql(record: Any) -> str:
    """Select the exact persisted hash needed after any MERGE/CAS."""

    if isinstance(record, BackfillCampaign):
        return (
            f"SELECT record_sha256, revision FROM {CAMPAIGN_TABLE} "
            f"WHERE campaign_id = '{record.campaign_id}'"
        )
    if isinstance(record, BackfillScopeState):
        scope_id = record.target.scope_id.replace("'", "''")
        return (
            f"SELECT record_sha256, revision FROM {SCOPE_TABLE} "
            f"WHERE campaign_id = '{record.campaign_id}' "
            f"AND scope_id = '{scope_id}'"
        )
    if isinstance(record, BackfillAttempt):
        return (
            f"SELECT record_sha256 FROM {ATTEMPT_TABLE} "
            f"WHERE attempt_id = '{record.attempt_id}'"
        )
    if isinstance(record, BackfillBatch):
        return (
            f"SELECT record_sha256, revision FROM {BATCH_TABLE} "
            f"WHERE batch_id = '{record.batch_id}'"
        )
    raise BackfillStateError("unsupported record type for readback")


def verify_record_readback(
    record: BackfillCampaign | BackfillScopeState | BackfillAttempt | BackfillBatch,
    rows: Iterable[Mapping[str, Any]],
) -> None:
    """Fail unless readback is exactly one row with the exact hash/revision."""

    values = tuple(rows)
    if len(values) != 1 or not isinstance(values[0], Mapping):
        raise BackfillStateError("record readback must return exactly one row")
    row = values[0]
    expected_hash = (
        canonical_sha256(campaign_storage_payload(record))
        if isinstance(record, BackfillCampaign)
        else record_sha256(record)
    )
    if _required_sha256("record_sha256", row.get("record_sha256")) != expected_hash:
        raise BackfillStateError("record readback hash mismatch")
    if isinstance(record, BackfillAttempt):
        return
    if "revision" not in row:
        raise BackfillStateError("record readback lacks revision")
    try:
        revision = int(row["revision"])
    except (TypeError, ValueError) as exc:
        raise BackfillStateError("record readback revision is invalid") from exc
    if revision != record.revision:
        raise BackfillStateError("record readback revision mismatch")


__all__ = [
    "ATTEMPT_TABLE",
    "BATCH_TABLE",
    "CAMPAIGN_TABLE",
    "CONTRACT_VERSION",
    "FIRST_SOURCE_RETRY_DELAY",
    "LEASE_GRACE",
    "MAX_SOURCE_ERRORS",
    "MAX_SOURCE_ATTEMPTS",
    "REPORT_SCHEMA_VERSION",
    "SCOPE_TABLE",
    "SECOND_SOURCE_RETRY_DELAY",
    "STALE_LEASE_AFTER",
    "TERMINAL_SCOPE_STATUSES",
    "UNAVAILABLE_CONFIRMATIONS_REQUIRED",
    "AttemptOutcome",
    "AttemptTransitionPlan",
    "BackfillAttempt",
    "BackfillBatch",
    "BackfillCampaign",
    "BackfillPlatformIncident",
    "BackfillScopeState",
    "BackfillStateError",
    "BatchStatus",
    "CampaignStatus",
    "CanonicalArtifact",
    "ClaimResult",
    "CompletionAccounting",
    "CompletionEvidence",
    "HistoricalScopeTarget",
    "ScopeStatus",
    "accounting_for",
    "apply_attempt",
    "attempt_from_mapping",
    "attempt_blocks_campaign",
    "attempt_merge_sql",
    "attempt_transition_merge_statements",
    "batch_from_mapping",
    "batch_merge_sql",
    "batch_transition_merge_sql",
    "campaign_from_mapping",
    "campaign_merge_sql",
    "campaign_select_sql",
    "campaign_transition_merge_sql",
    "canonical_json",
    "canonical_sha256",
    "claim_scopes",
    "claim_merge_statements",
    "complete_campaign",
    "completion_report",
    "ddl_statements",
    "freeze_historical_targets",
    "heartbeat_scope",
    "initial_scope_chunk_merge_sql",
    "initial_scope_chunk_readback_sql",
    "is_stale_lease",
    "mark_scope_dq_complete",
    "parse_attempt_row",
    "parse_batch_row",
    "parse_campaign_row",
    "parse_campaign_pointer_row",
    "parse_scope_row",
    "platform_incident_from_mapping",
    "plan_attempt_transition",
    "reclaim_stale_lease",
    "record_payload",
    "record_readback_sql",
    "record_sha256",
    "recover_batch_scopes",
    "resume_platform_scopes",
    "scope_from_mapping",
    "scope_merge_sql",
    "scope_select_sql",
    "scope_transition_merge_sql",
    "stable_attempt_id",
    "stable_batch_id",
    "stable_campaign_id",
    "stable_platform_incident_id",
    "verify_completion_evidence",
    "verify_completion_report",
    "verify_initial_scope_chunk_readback",
    "verify_record_readback",
]
