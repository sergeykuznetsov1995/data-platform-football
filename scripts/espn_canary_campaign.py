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
from urllib.parse import unquote, urlsplit

from scrapers.espn.canary_campaign import (
    CampaignError,
    CampaignIdentity,
    CampaignLedger,
)


UTC = timezone.utc
EXACT_CANARY_SCOPE_COUNT = 181
_SHA256_LENGTH = 64


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


def _required(value: object, field: str) -> str:
    if type(value) is not str or not value.strip():
        raise CampaignError(f"{field} must be a non-empty string")
    return value


def _sha256(value: object, field: str) -> str:
    result = _required(value, field)
    if len(result) != _SHA256_LENGTH or any(
        character not in "0123456789abcdef" for character in result
    ):
        raise CampaignError(f"{field} must be a lowercase SHA-256")
    return result


def _artifact_ref(value: object, field: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != {"uri", "sha256"}:
        raise CampaignError(f"{field} must contain exactly uri and sha256")
    return {
        "uri": _required(value["uri"], f"{field}.uri"),
        "sha256": _sha256(value["sha256"], f"{field}.sha256"),
    }


def _local_path(uri: str, field: str) -> Path:
    parsed = urlsplit(_required(uri, field))
    if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
        raise CampaignError(f"{field} must be a local file URI")
    return Path(unquote(parsed.path))


def _read_evidence(
    ref: object, *, field: str, kind: str | None = None
) -> tuple[dict[str, object], Path]:
    exact_ref = _artifact_ref(ref, field)
    path = _local_path(exact_ref["uri"], f"{field}.uri")
    try:
        body = path.read_bytes()
        payload = json.loads(body.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CampaignError(f"{field} is unreadable or malformed") from exc
    if hashlib.sha256(body).hexdigest() != exact_ref["sha256"]:
        raise CampaignError(f"{field} hash mismatch")
    if not isinstance(payload, dict) or _canonical_bytes(payload) != body:
        raise CampaignError(f"{field} is not canonical JSON")
    if kind is not None and payload.get("kind") != kind:
        raise CampaignError(f"{field} kind must be {kind}")
    return payload, path


def _claim_state(
    claim_ref: object,
) -> tuple[dict[str, object], CampaignLedger, Path, object]:
    claim, _ = _read_evidence(
        claim_ref,
        field="claim_ref",
        kind="espn-canary-claim-evidence-v1",
    )
    if set(claim) != {
        "kind",
        "schema_version",
        "ledger_ref",
        "ledger",
        "campaign",
        "attempt",
    } or claim.get("schema_version") != 1:
        raise CampaignError("claim evidence schema is malformed")
    try:
        claimed_ledger = CampaignLedger.from_dict(claim["ledger"])
    except CampaignError:
        raise
    attempt = claimed_ledger.attempts[-1] if claimed_ledger.attempts else None
    if (
        attempt is None
        or attempt.status != "active"
        or attempt.to_dict() != claim["attempt"]
        or attempt.campaign.to_dict() != claim["campaign"]
    ):
        raise CampaignError("claim evidence is not the latest active attempt")
    ledger_ref = _artifact_ref(claim["ledger_ref"], "claim ledger_ref")
    ledger_path = _local_path(ledger_ref["uri"], "claim ledger_ref.uri")
    return claim, claimed_ledger, ledger_path, attempt


def _evidence_dir(ledger_path: Path) -> Path:
    return ledger_path.with_name(ledger_path.name + ".evidence")


def _attempt_marker_path(ledger_path: Path, kind: str, attempt_id: str) -> Path:
    safe_attempt_id = _required(attempt_id, "attempt_id")
    if "/" in safe_attempt_id or safe_attempt_id in {".", ".."}:
        raise CampaignError("attempt_id is unsafe")
    return _evidence_dir(ledger_path) / f"{kind}-{safe_attempt_id}.json"


def _persist_immutable_at(
    path: Path, payload: Mapping[str, object]
) -> dict[str, str]:
    body = _canonical_bytes(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        if path.read_bytes() != body:
            raise CampaignError("immutable campaign evidence conflicts")
    else:
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except Exception:
            path.unlink(missing_ok=True)
            raise
    os.chmod(path, 0o600)
    return {"uri": path.resolve().as_uri(), "sha256": hashlib.sha256(body).hexdigest()}


def _assert_claim_matches_current(
    claim: Mapping[str, object], ledger: CampaignLedger
) -> None:
    if ledger.to_dict() != claim["ledger"]:
        raise CampaignError("claim ledger changed after claim")


def _finish_marker_path(ledger_path: Path, attempt_id: str) -> Path:
    return _attempt_marker_path(
        ledger_path, "espn-canary-finish-evidence-v1", attempt_id
    )


def _consumption_marker_path(ledger_path: Path, attempt_id: str) -> Path:
    return _attempt_marker_path(
        ledger_path, "espn-canary-claim-consumption-v1", attempt_id
    )


def _validate_consumption_payload(
    payload: Mapping[str, object], expected: Mapping[str, object]
) -> dict[str, object]:
    fields = {
        "kind",
        "schema_version",
        "claim_ref",
        "dag_id",
        "run_id",
        "admission_identity",
        "campaign_id",
        "attempt_id",
        "ordinal",
        "consumed_at",
    }
    if (
        set(payload) != fields
        or payload.get("kind") != "espn-canary-claim-consumption-v1"
        or payload.get("schema_version") != 1
        or any(payload.get(key) != value for key, value in expected.items())
    ):
        raise CampaignError("canary consumption binding differs")
    try:
        consumed_at = datetime.fromisoformat(str(payload["consumed_at"]))
    except ValueError as exc:
        raise CampaignError("canary consumption timestamp is malformed") from exc
    if consumed_at.tzinfo is None:
        raise CampaignError("canary consumption timestamp is malformed")
    return dict(payload)


def _validate_finish_payload(
    payload: Mapping[str, object], *, ledger_path: Path, attempt_id: str
) -> tuple[CampaignLedger, object]:
    fields = {
        "kind",
        "schema_version",
        "ledger_ref",
        "ledger",
        "campaign",
        "attempt",
        "successful",
        "terminal_ref",
    }
    if (
        set(payload) != fields
        or payload.get("kind") != "espn-canary-finish-evidence-v1"
        or payload.get("schema_version") != 1
        or type(payload.get("successful")) is not bool
    ):
        raise CampaignError("campaign finish evidence schema is malformed")
    terminal_ledger = CampaignLedger.from_dict(payload["ledger"])
    if not terminal_ledger.attempts:
        raise CampaignError("campaign finish ledger is empty")
    terminal_attempt = terminal_ledger.attempts[-1]
    ledger_ref = _artifact_ref(payload["ledger_ref"], "finish ledger_ref")
    expected_ledger_ref = {
        "uri": ledger_path.resolve().as_uri(),
        "sha256": hashlib.sha256(_canonical_bytes(payload["ledger"])).hexdigest(),
    }
    if (
        ledger_ref != expected_ledger_ref
        or terminal_attempt.attempt_id != attempt_id
        or terminal_attempt.status
        != ("successful" if payload["successful"] else "failed")
        or terminal_attempt.to_dict() != payload["attempt"]
        or terminal_attempt.campaign.to_dict() != payload["campaign"]
        or terminal_attempt.terminal_ref != payload["terminal_ref"]
    ):
        raise CampaignError("campaign finish evidence identity is malformed")
    return terminal_ledger, terminal_attempt


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
    evidence_path = _evidence_dir(ledger_path) / (
        f"{payload['kind']}-{digest}.json"
    )
    return _persist_immutable_at(evidence_path, payload)


def consume_campaign_claim(
    *,
    claim_ref: Mapping[str, str],
    dag_id: str,
    run_id: str,
    admission_identity: str,
    now: datetime | None = None,
) -> dict[str, str]:
    """Atomically bind one active claim to one exact admission identity."""

    exact_claim_ref = _artifact_ref(claim_ref, "claim_ref")
    claim, _, ledger_path, claimed_attempt = _claim_state(exact_claim_ref)
    binding = {
        "claim_ref": exact_claim_ref,
        "dag_id": _required(dag_id, "dag_id"),
        "run_id": _required(run_id, "run_id"),
        "admission_identity": _sha256(
            admission_identity, "admission_identity"
        ),
    }
    marker_path = _consumption_marker_path(
        ledger_path, claimed_attempt.attempt_id
    )
    finish_path = _finish_marker_path(ledger_path, claimed_attempt.attempt_id)
    with _locked_ledger(ledger_path) as current_ledger:
        claim, _, _, current_attempt = _claim_state(exact_claim_ref)
        _assert_claim_matches_current(claim, current_ledger)
        if current_attempt.attempt_id != claimed_attempt.attempt_id:
            raise CampaignError("claim attempt identity changed")
        if finish_path.exists():
            raise CampaignError("canary claim is finished and revoked")
        if marker_path.exists():
            marker_ref = {
                "uri": marker_path.resolve().as_uri(),
                "sha256": hashlib.sha256(marker_path.read_bytes()).hexdigest(),
            }
            marker, _ = _read_evidence(
                marker_ref,
                field="consumption_ref",
                kind="espn-canary-claim-consumption-v1",
            )
            _validate_consumption_payload(
                marker,
                {
                    "claim_ref": marker.get("claim_ref"),
                    "dag_id": marker.get("dag_id"),
                    "run_id": marker.get("run_id"),
                    "admission_identity": marker.get("admission_identity"),
                    "campaign_id": current_attempt.campaign_id,
                    "attempt_id": current_attempt.attempt_id,
                    "ordinal": current_attempt.ordinal,
                },
            )
            if {
                key: marker[key]
                for key in (
                    "claim_ref",
                    "dag_id",
                    "run_id",
                    "admission_identity",
                )
            } != binding:
                raise CampaignError("canary claim already consumed by another admission")
            return marker_ref
        consumed_at = now or datetime.now(UTC)
        if consumed_at.tzinfo is None:
            raise CampaignError("consumption time must be timezone-aware")
        consumed_at = consumed_at.astimezone(UTC)
        payload = {
            "kind": "espn-canary-claim-consumption-v1",
            "schema_version": 1,
            **binding,
            "campaign_id": current_attempt.campaign_id,
            "attempt_id": current_attempt.attempt_id,
            "ordinal": current_attempt.ordinal,
            "consumed_at": consumed_at.isoformat(),
        }
        return _persist_immutable_at(marker_path, payload)


def validate_campaign_consumption(
    *,
    claim_ref: Mapping[str, str],
    consumption_ref: Mapping[str, str],
    dag_id: str,
    run_id: str,
    admission_identity: str,
) -> dict[str, object]:
    """Strict-read the exact single-use binding and monotonic revocation state."""

    exact_claim_ref = _artifact_ref(claim_ref, "claim_ref")
    exact_consumption_ref = _artifact_ref(consumption_ref, "consumption_ref")
    claim, _, ledger_path, claimed_attempt = _claim_state(exact_claim_ref)
    marker_path = _consumption_marker_path(
        ledger_path, claimed_attempt.attempt_id
    )
    finish_path = _finish_marker_path(ledger_path, claimed_attempt.attempt_id)
    if exact_consumption_ref["uri"] != marker_path.resolve().as_uri():
        raise CampaignError("consumption reference differs from attempt marker")
    with _locked_ledger(ledger_path) as current_ledger:
        if finish_path.exists():
            finish_body = finish_path.read_bytes()
            finish_ref = {
                "uri": finish_path.resolve().as_uri(),
                "sha256": hashlib.sha256(finish_body).hexdigest(),
            }
            finish, _ = _read_evidence(
                finish_ref,
                field="finish_ref",
                kind="espn-canary-finish-evidence-v1",
            )
            _validate_finish_payload(
                finish,
                ledger_path=ledger_path,
                attempt_id=claimed_attempt.attempt_id,
            )
            raise CampaignError("canary claim is finished and revoked")
        _assert_claim_matches_current(claim, current_ledger)
        consumption, _ = _read_evidence(
            exact_consumption_ref,
            field="consumption_ref",
            kind="espn-canary-claim-consumption-v1",
        )
        expected = {
            "claim_ref": exact_claim_ref,
            "dag_id": _required(dag_id, "dag_id"),
            "run_id": _required(run_id, "run_id"),
            "admission_identity": _sha256(
                admission_identity, "admission_identity"
            ),
            "campaign_id": claimed_attempt.campaign_id,
            "attempt_id": claimed_attempt.attempt_id,
            "ordinal": claimed_attempt.ordinal,
        }
        return _validate_consumption_payload(consumption, expected)


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
        finish_path = _finish_marker_path(path, attempt_id)
        if finish_path.exists():
            existing_ref = {
                "uri": finish_path.resolve().as_uri(),
                "sha256": hashlib.sha256(finish_path.read_bytes()).hexdigest(),
            }
            existing, _ = _read_evidence(
                existing_ref,
                field="finish_ref",
                kind="espn-canary-finish-evidence-v1",
            )
            terminal_ledger, terminal_attempt = _validate_finish_payload(
                existing,
                ledger_path=path,
                attempt_id=attempt_id,
            )
            if (
                existing["successful"] is not successful
                or existing.get("terminal_ref") != dict(terminal_ref)
            ):
                raise CampaignError("immutable campaign finish evidence conflicts")
            ledger_ref = _persist(path, terminal_ledger)
            if ledger_ref != existing["ledger_ref"]:
                raise CampaignError("campaign finish ledger reference drift")
            return {
                "campaign_id": terminal_attempt.campaign_id,
                "attempt_id": terminal_attempt.attempt_id,
                "status": "successful" if successful else "failed",
                "terminal_ref": dict(terminal_ref),
                "ledger_ref": ledger_ref,
                "finish_ref": existing_ref,
            }
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
        terminal_attempt = ledger.attempts[-1]
        ledger_body = _canonical_bytes(ledger.to_dict())
        ledger_ref = {
            "uri": path.resolve().as_uri(),
            "sha256": hashlib.sha256(ledger_body).hexdigest(),
        }
        finish_ref = _persist_immutable_at(
            finish_path,
            {
                "kind": "espn-canary-finish-evidence-v1",
                "schema_version": 1,
                "ledger_ref": ledger_ref,
                "ledger": ledger.to_dict(),
                "campaign": terminal_attempt.campaign.to_dict(),
                "attempt": terminal_attempt.to_dict(),
                "successful": successful,
                "terminal_ref": dict(terminal_ref),
            },
        )
        persisted_ref = _persist(path, ledger)
        if persisted_ref != ledger_ref:
            raise CampaignError("campaign finish ledger persistence drift")
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
    "consume_campaign_claim",
    "finish_campaign_attempt",
    "validate_campaign_consumption",
]
