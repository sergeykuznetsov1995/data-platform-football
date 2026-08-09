"""Fail-closed identity and attempt accounting for ESPN release canaries.

The ledger is operational state, not a seventh Bronze object.  Callers persist
its canonical mapping under their existing guarded operator journal.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Mapping, Sequence


UTC = timezone.utc
_COMMIT_RE = re.compile(r"[0-9a-f]{40}")
_SHA_RE = re.compile(r"[0-9a-f]{64}")
_SCOPE_RE = re.compile(r"[1-9][0-9]*:[1-9][0-9]*")
_STATUSES = frozenset({"active", "failed", "successful"})


class CampaignError(RuntimeError):
    """The canary campaign state cannot safely authorize an attempt."""


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _required(value: object, field: str) -> str:
    if type(value) is not str or not value.strip():
        raise CampaignError(f"{field} must be a non-empty string")
    return value


def _sha(value: object, field: str) -> str:
    result = _required(value, field)
    if _SHA_RE.fullmatch(result) is None:
        raise CampaignError(f"{field} must be a lowercase SHA-256")
    return result


def _utc(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise CampaignError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _artifact_ref(value: object, field: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != {"uri", "sha256"}:
        raise CampaignError(f"{field} must contain exactly uri and sha256")
    return {
        "uri": _required(value["uri"], f"{field}.uri"),
        "sha256": _sha(value["sha256"], f"{field}.sha256"),
    }


@dataclass(frozen=True, slots=True)
class CampaignIdentity:
    release_commit: str
    release_tree_sha256: str
    registry_signature: str
    target_scope_sha256: str
    target_scope_ids: tuple[str, ...]
    campaign_id: str

    @classmethod
    def create(
        cls,
        *,
        release_commit: str,
        release_tree_sha256: str,
        registry_signature: str,
        target_scope_ids: Sequence[str],
    ) -> "CampaignIdentity":
        commit = _required(release_commit, "release_commit")
        if _COMMIT_RE.fullmatch(commit) is None:
            raise CampaignError("release_commit must be a lowercase 40-hex commit")
        tree = _sha(release_tree_sha256, "release_tree_sha256")
        registry = _sha(registry_signature, "registry_signature")
        if not isinstance(target_scope_ids, (list, tuple)):
            raise CampaignError("target_scope_ids must be a sequence")
        scopes = tuple(sorted(target_scope_ids))
        if (
            not scopes
            or len(scopes) != len(set(scopes))
            or any(type(item) is not str or _SCOPE_RE.fullmatch(item) is None for item in scopes)
        ):
            raise CampaignError("target_scope_ids must be sorted unique ESPN scopes")
        target_sha = hashlib.sha256(_canonical_bytes(list(scopes))).hexdigest()
        identity = {
            "release_commit": commit,
            "release_tree_sha256": tree,
            "registry_signature": registry,
            "target_scope_sha256": target_sha,
        }
        campaign_id = hashlib.sha256(_canonical_bytes(identity)).hexdigest()
        return cls(
            release_commit=commit,
            release_tree_sha256=tree,
            registry_signature=registry,
            target_scope_sha256=target_sha,
            target_scope_ids=scopes,
            campaign_id=campaign_id,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "release_commit": self.release_commit,
            "release_tree_sha256": self.release_tree_sha256,
            "registry_signature": self.registry_signature,
            "target_scope_sha256": self.target_scope_sha256,
            "target_scope_ids": list(self.target_scope_ids),
            "campaign_id": self.campaign_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> "CampaignIdentity":
        fields = {
            "release_commit",
            "release_tree_sha256",
            "registry_signature",
            "target_scope_sha256",
            "target_scope_ids",
            "campaign_id",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise CampaignError("campaign identity is malformed")
        created = cls.create(
            release_commit=value["release_commit"],
            release_tree_sha256=value["release_tree_sha256"],
            registry_signature=value["registry_signature"],
            target_scope_ids=value["target_scope_ids"],
        )
        if created.to_dict() != dict(value):
            raise CampaignError("campaign identity is malformed or drifted")
        return created


@dataclass(frozen=True, slots=True)
class CampaignAttempt:
    campaign: CampaignIdentity
    ordinal: int | None
    attempt_id: str | None
    status: str
    claimed_at: datetime
    completed_at: datetime | None = None
    predecessor_campaign_id: str | None = None
    predecessor_failure_ref: Mapping[str, str] | None = None
    remediation: str | None = None
    terminal_ref: Mapping[str, str] | None = None

    @property
    def campaign_id(self) -> str:
        return self.campaign.campaign_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign": self.campaign.to_dict(),
            "ordinal": self.ordinal,
            "attempt_id": self.attempt_id,
            "status": self.status,
            "claimed_at": self.claimed_at.isoformat(),
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at is not None else None
            ),
            "predecessor_campaign_id": self.predecessor_campaign_id,
            "predecessor_failure_ref": (
                dict(self.predecessor_failure_ref)
                if self.predecessor_failure_ref is not None
                else None
            ),
            "remediation": self.remediation,
            "terminal_ref": (
                dict(self.terminal_ref) if self.terminal_ref is not None else None
            ),
        }

    @classmethod
    def from_dict(cls, value: object) -> "CampaignAttempt":
        fields = {
            "campaign",
            "ordinal",
            "attempt_id",
            "status",
            "claimed_at",
            "completed_at",
            "predecessor_campaign_id",
            "predecessor_failure_ref",
            "remediation",
            "terminal_ref",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise CampaignError("campaign attempt is malformed")
        try:
            claimed = datetime.fromisoformat(value["claimed_at"])
            completed = (
                datetime.fromisoformat(value["completed_at"])
                if value["completed_at"] is not None
                else None
            )
        except (TypeError, ValueError) as exc:
            raise CampaignError("campaign attempt timestamp is malformed") from exc
        campaign = CampaignIdentity.from_dict(value["campaign"])
        ordinal = value["ordinal"]
        status = value["status"]
        if type(ordinal) is not int or not 1 <= ordinal <= 3:
            raise CampaignError("campaign attempt ordinal is malformed")
        attempt_id = f"{campaign.campaign_id}-ordinal{ordinal:03d}"
        if value["attempt_id"] != attempt_id or status not in _STATUSES:
            raise CampaignError("campaign attempt identity or status is malformed")
        terminal_ref = value["terminal_ref"]
        if status == "active":
            if completed is not None or terminal_ref is not None:
                raise CampaignError("active campaign attempt is malformed")
        elif completed is None or terminal_ref is None:
            raise CampaignError("terminal campaign attempt is malformed")
        return cls(
            campaign=campaign,
            ordinal=ordinal,
            attempt_id=attempt_id,
            status=status,
            claimed_at=_utc(claimed, "claimed_at"),
            completed_at=(
                _utc(completed, "completed_at") if completed is not None else None
            ),
            predecessor_campaign_id=value["predecessor_campaign_id"],
            predecessor_failure_ref=(
                _artifact_ref(
                    value["predecessor_failure_ref"], "predecessor_failure_ref"
                )
                if value["predecessor_failure_ref"] is not None
                else None
            ),
            remediation=(
                _required(value["remediation"], "remediation")
                if value["remediation"] is not None
                else None
            ),
            terminal_ref=(
                _artifact_ref(terminal_ref, "terminal_ref")
                if terminal_ref is not None
                else None
            ),
        )


class CampaignLedger:
    """Strict in-memory state machine; persist ``to_dict()`` atomically."""

    def __init__(self, attempts: Sequence[CampaignAttempt] = ()) -> None:
        self._attempts = list(attempts)
        self._validate_history()

    @classmethod
    def empty(cls) -> "CampaignLedger":
        return cls()

    @property
    def attempts(self) -> tuple[CampaignAttempt, ...]:
        return tuple(self._attempts)

    def _validate_history(self) -> None:
        by_campaign: dict[str, list[CampaignAttempt]] = {}
        for attempt in self._attempts:
            by_campaign.setdefault(attempt.campaign_id, []).append(attempt)
        for items in by_campaign.values():
            ordinals = [item.ordinal for item in items]
            if ordinals != list(range(1, len(items) + 1)) or len(items) > 3:
                raise CampaignError("campaign ledger ordinal history is malformed")
            if any(item.status == "active" for item in items[:-1]):
                raise CampaignError("campaign ledger active history is malformed")
            if any(item.status == "successful" for item in items[:-1]):
                raise CampaignError("campaign ledger successful history is malformed")
        active = [item for item in self._attempts if item.status == "active"]
        if len(active) > 1:
            raise CampaignError("campaign ledger has multiple active attempts")
        for index, attempt in enumerate(self._attempts):
            predecessor_fields = (
                attempt.predecessor_campaign_id,
                attempt.predecessor_failure_ref,
                attempt.remediation,
            )
            previous = self._attempts[index - 1] if index else None
            same_campaign = (
                previous is not None
                and previous.campaign_id == attempt.campaign_id
            )
            if index == 0 or same_campaign:
                if any(item is not None for item in predecessor_fields):
                    raise CampaignError("campaign ledger predecessor link is malformed")
                continue
            if (
                attempt.ordinal != 1
                or previous is None
                or previous.status != "failed"
                or previous.terminal_ref is None
                or attempt.predecessor_campaign_id != previous.campaign_id
                or attempt.predecessor_failure_ref != previous.terminal_ref
                or not attempt.remediation
            ):
                raise CampaignError("campaign ledger successor link is malformed")

    def _same_release_drift(self, identity: CampaignIdentity) -> None:
        for attempt in self._attempts:
            previous = attempt.campaign
            if previous.release_commit != identity.release_commit:
                continue
            if previous.release_tree_sha256 != identity.release_tree_sha256:
                raise CampaignError("release tree drift within canary campaign")
            if previous.registry_signature != identity.registry_signature:
                raise CampaignError("registry drift within canary campaign")
            if previous.target_scope_sha256 != identity.target_scope_sha256:
                raise CampaignError("target drift within canary campaign")

    def claim(
        self,
        identity: CampaignIdentity,
        *,
        now: datetime,
        guard_only: bool = False,
        predecessor_failure_ref: Mapping[str, str] | None = None,
        remediation: str | None = None,
    ) -> CampaignAttempt:
        if not isinstance(identity, CampaignIdentity):
            raise CampaignError("campaign identity is malformed")
        CampaignIdentity.from_dict(identity.to_dict())
        claimed_at = _utc(now, "now")
        self._same_release_drift(identity)
        same = [
            item for item in self._attempts if item.campaign_id == identity.campaign_id
        ]
        if same and same[-1].status == "active":
            raise CampaignError("canary campaign already has an active attempt")
        if any(item.status == "successful" for item in same):
            raise CampaignError("canary campaign is already successful")
        if len(same) >= 3:
            raise CampaignError("canary campaign already used three attempts")

        predecessor_id = None
        predecessor_ref = None
        predecessor_remediation = None
        if not same and self._attempts:
            predecessor = self._attempts[-1]
            if predecessor.status == "active":
                raise CampaignError("active predecessor campaign blocks successor")
            if predecessor.status == "successful":
                raise CampaignError("successful predecessor campaign blocks successor")
            if predecessor.status != "failed" or predecessor.terminal_ref is None:
                raise CampaignError("malformed predecessor campaign blocks successor")
            predecessor_ref = _artifact_ref(
                predecessor_failure_ref, "predecessor_failure_ref"
            )
            if predecessor_ref != dict(predecessor.terminal_ref):
                raise CampaignError("successor predecessor failure reference differs")
            predecessor_remediation = _required(remediation, "remediation")
            predecessor_id = predecessor.campaign_id
        elif predecessor_failure_ref is not None or remediation is not None:
            raise CampaignError("first/same campaign cannot claim predecessor remediation")

        ordinal = len(same) + 1
        attempt = CampaignAttempt(
            campaign=identity,
            ordinal=None if guard_only else ordinal,
            attempt_id=(
                None
                if guard_only
                else f"{identity.campaign_id}-ordinal{ordinal:03d}"
            ),
            status="active",
            claimed_at=claimed_at,
            predecessor_campaign_id=predecessor_id,
            predecessor_failure_ref=predecessor_ref,
            remediation=predecessor_remediation,
        )
        if not guard_only:
            candidate = CampaignLedger((*self._attempts, attempt))
            self._attempts = list(candidate.attempts)
        return attempt

    def _complete(
        self,
        attempt: CampaignAttempt,
        *,
        status: str,
        terminal_ref: Mapping[str, str],
        now: datetime,
    ) -> "CampaignLedger":
        if status not in {"failed", "successful"}:
            raise CampaignError("invalid terminal campaign status")
        if not self._attempts or self._attempts[-1] != attempt:
            raise CampaignError("terminal transition does not match active attempt")
        if attempt.status != "active" or attempt.ordinal is None:
            raise CampaignError("only a consumed active attempt may become terminal")
        completed = _utc(now, "now")
        if completed < attempt.claimed_at:
            raise CampaignError("campaign completion predates its claim")
        self._attempts[-1] = replace(
            attempt,
            status=status,
            completed_at=completed,
            terminal_ref=_artifact_ref(terminal_ref, "terminal_ref"),
        )
        return self

    def fail(
        self,
        attempt: CampaignAttempt,
        *,
        failure_ref: Mapping[str, str],
        now: datetime,
    ) -> "CampaignLedger":
        return self._complete(
            attempt, status="failed", terminal_ref=failure_ref, now=now
        )

    def succeed(
        self,
        attempt: CampaignAttempt,
        *,
        success_receipt_ref: Mapping[str, str],
        now: datetime,
    ) -> "CampaignLedger":
        return self._complete(
            attempt,
            status="successful",
            terminal_ref=success_receipt_ref,
            now=now,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "espn-canary-campaign-ledger-v1",
            "schema_version": 1,
            "attempts": [item.to_dict() for item in self._attempts],
        }

    @classmethod
    def from_dict(cls, value: object) -> "CampaignLedger":
        if (
            not isinstance(value, Mapping)
            or set(value) != {"kind", "schema_version", "attempts"}
            or value.get("kind") != "espn-canary-campaign-ledger-v1"
            or value.get("schema_version") != 1
            or not isinstance(value.get("attempts"), list)
        ):
            raise CampaignError("canary campaign ledger is malformed")
        try:
            return cls(tuple(CampaignAttempt.from_dict(item) for item in value["attempts"]))
        except CampaignError:
            raise
        except Exception as exc:  # pragma: no cover - defensive normalization
            raise CampaignError("canary campaign ledger is malformed") from exc


__all__ = [
    "CampaignAttempt",
    "CampaignError",
    "CampaignIdentity",
    "CampaignLedger",
]
