"""Atomic host-side operator primitives for ESPN canary attempt accounting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Iterator, Mapping, Sequence

from scrapers.espn.canary_campaign import (
    CampaignError,
    CampaignIdentity,
    CampaignLedger,
)


UTC = timezone.utc
EXACT_CANARY_SCOPE_COUNT = 181


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


@contextmanager
def _locked_ledger(path: Path) -> Iterator[CampaignLedger]:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(path.name + ".lock")
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.chmod(lock_path, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise CampaignError("persisted canary campaign ledger is malformed") from exc
            ledger = CampaignLedger.from_dict(payload)
        else:
            ledger = CampaignLedger.empty()
        yield ledger
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _persist(path: Path, ledger: CampaignLedger) -> dict[str, str]:
    body = _canonical_bytes(ledger.to_dict())
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return {"uri": path.resolve().as_uri(), "sha256": hashlib.sha256(body).hexdigest()}


def _persist_immutable_evidence(
    ledger_path: Path, payload: Mapping[str, object]
) -> dict[str, str]:
    body = _canonical_bytes(payload)
    digest = hashlib.sha256(body).hexdigest()
    evidence_path = ledger_path.with_name(ledger_path.name + ".evidence") / (
        f"{payload['kind']}-{digest}.json"
    )
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(
            evidence_path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
    except FileExistsError:
        if evidence_path.read_bytes() != body:
            raise CampaignError("content-addressed campaign evidence conflicts")
    else:
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            directory = os.open(evidence_path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except Exception:
            evidence_path.unlink(missing_ok=True)
            raise
    os.chmod(evidence_path, 0o600)
    return {"uri": evidence_path.resolve().as_uri(), "sha256": digest}


def claim_campaign_attempt(
    *,
    ledger_path: str | Path,
    release_commit: str,
    release_tree_sha256: str,
    registry_signature: str,
    target_scope_ids: Sequence[str],
    guard_only: bool = False,
    predecessor_failure_ref: Mapping[str, str] | None = None,
    remediation: str | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Validate or atomically consume one exact all-181 campaign ordinal."""

    if len(target_scope_ids) != EXACT_CANARY_SCOPE_COUNT:
        raise CampaignError("ESPN canary target must contain exact 181 scopes")
    identity = CampaignIdentity.create(
        release_commit=release_commit,
        release_tree_sha256=release_tree_sha256,
        registry_signature=registry_signature,
        target_scope_ids=target_scope_ids,
    )
    path = Path(ledger_path)
    with _locked_ledger(path) as ledger:
        attempt = ledger.claim(
            identity,
            now=now or datetime.now(UTC),
            guard_only=guard_only,
            predecessor_failure_ref=predecessor_failure_ref,
            remediation=remediation,
        )
        ledger_ref = None if guard_only else _persist(path, ledger)
        claim_ref = (
            None
            if guard_only
            else _persist_immutable_evidence(
                path,
                {
                    "kind": "espn-canary-claim-evidence-v1",
                    "schema_version": 1,
                    "ledger_ref": ledger_ref,
                    "ledger": ledger.to_dict(),
                    "campaign": identity.to_dict(),
                    "attempt": attempt.to_dict(),
                },
            )
        )
    return {
        "guard_only": guard_only,
        "campaign": identity.to_dict(),
        "attempt": attempt.to_dict(),
        "ledger_ref": ledger_ref,
        "claim_ref": claim_ref,
    }


def finish_campaign_attempt(
    *,
    ledger_path: str | Path,
    attempt_id: str,
    terminal_ref: Mapping[str, str],
    successful: bool,
    now: datetime | None = None,
) -> dict[str, object]:
    """Atomically attach the failed/success receipt to the active attempt."""

    path = Path(ledger_path)
    with _locked_ledger(path) as ledger:
        active = [item for item in ledger.attempts if item.status == "active"]
        if len(active) != 1 or active[0].attempt_id != attempt_id:
            raise CampaignError("active canary attempt identity mismatch")
        attempt = active[0]
        if successful:
            ledger.succeed(
                attempt,
                success_receipt_ref=terminal_ref,
                now=now or datetime.now(UTC),
            )
        else:
            ledger.fail(
                attempt,
                failure_ref=terminal_ref,
                now=now or datetime.now(UTC),
            )
        ledger_ref = _persist(path, ledger)
        terminal_attempt = ledger.attempts[-1]
        finish_ref = _persist_immutable_evidence(
            path,
            {
                "kind": "espn-canary-finish-evidence-v1",
                "schema_version": 1,
                "ledger_ref": ledger_ref,
                "ledger": ledger.to_dict(),
                "campaign": terminal_attempt.campaign.to_dict(),
                "attempt": terminal_attempt.to_dict(),
            },
        )
    return {
        "campaign_id": attempt.campaign_id,
        "attempt_id": attempt.attempt_id,
        "status": "successful" if successful else "failed",
        "terminal_ref": dict(terminal_ref),
        "ledger_ref": ledger_ref,
        "finish_ref": finish_ref,
    }


__all__ = [
    "EXACT_CANARY_SCOPE_COUNT",
    "claim_campaign_attempt",
    "finish_campaign_attempt",
]
