"""Crash-consistent ESPN canary attempt accounting and guarded host CLI."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
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
DEFAULT_CANARY_STATE_ROOT = Path("/durable/espn/canary-state")
DEFAULT_CANARY_LEDGER_PATH = DEFAULT_CANARY_STATE_ROOT / "campaigns.json"
CANARY_STATE_ROOT_ENV = "ESPN_CANARY_STATE_ROOT"
_SHA256_LENGTH = 64
_CLAIM_KIND = "espn-canary-claim-evidence-v1"
_CONSUMPTION_KIND = "espn-canary-claim-consumption-v1"
_FINISH_KIND = "espn-canary-finish-evidence-v1"
_STATE_ROOT_MODE = 0o770
_EVIDENCE_DIR_MODE = 0o1770
_EVIDENCE_FILE_MODE = 0o440
_LEDGER_MODE = 0o660


def _persistence_boundary(_name: str) -> None:
    """No-op fault-injection seam immediately after one durable boundary."""


def _configured_state_root() -> Path | None:
    value = os.environ.get(CANARY_STATE_ROOT_ENV)
    if value is None:
        return None
    if not value or value != value.strip():
        raise CampaignError(f"{CANARY_STATE_ROOT_ENV} must be an absolute path")
    path = Path(value)
    if not path.is_absolute():
        raise CampaignError(f"{CANARY_STATE_ROOT_ENV} must be an absolute path")
    return path


def _without_symlink_components(path: Path, field: str) -> Path:
    lexical = Path(os.path.abspath(path))
    for component in (lexical, *lexical.parents):
        try:
            details = component.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise CampaignError(f"{field} path components are unreadable") from exc
        if stat.S_ISLNK(details.st_mode):
            raise CampaignError(f"{field} must not contain symlink components")
    return lexical


def _assert_in_configured_state_root(path: Path, field: str) -> Path:
    lexical = _without_symlink_components(path, field)
    resolved = lexical.resolve()
    root = _configured_state_root()
    if root is not None:
        resolved_root = _require_shared_state_root(root)
        if (
            lexical != resolved_root
            and resolved_root not in lexical.parents
            or resolved != resolved_root
            and resolved_root not in resolved.parents
        ):
            raise CampaignError(f"{field} must stay under {CANARY_STATE_ROOT_ENV}")
    return resolved


def _require_shared_state_root(path: Path) -> Path:
    original = _without_symlink_components(path, "canary state root")
    try:
        details = original.lstat()
    except OSError as exc:
        raise CampaignError("canary state root is missing or unreadable") from exc
    if (
        not stat.S_ISDIR(details.st_mode)
        or original.is_symlink()
        or details.st_uid != 0
        or details.st_gid != 0
        or stat.S_IMODE(details.st_mode) != _STATE_ROOT_MODE
    ):
        raise CampaignError(
            "canary state root must be a non-symlink root:0 directory with mode 0770"
        )
    return original.resolve()


def _cli_ledger_path(value: str) -> Path:
    candidate = Path(_required(value, "ledger_path"))
    if not candidate.is_absolute():
        raise CampaignError("ledger_path must be absolute")
    root = _require_shared_state_root(
        _configured_state_root() or DEFAULT_CANARY_STATE_ROOT
    )
    resolved = _assert_in_configured_state_root(candidate, "ledger_path")
    expected = root / DEFAULT_CANARY_LEDGER_PATH.name
    if resolved != expected:
        raise CampaignError(f"ledger_path must be the canonical canary ledger {expected}")
    return expected


def _directory_descriptor(path: Path, field: str) -> int:
    path = _without_symlink_components(path, field)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
        details = os.fstat(descriptor)
    except OSError as exc:
        raise CampaignError(f"{field} is missing or unsafe") from exc
    if not stat.S_ISDIR(details.st_mode):
        os.close(descriptor)
        raise CampaignError(f"{field} is not a directory")
    return descriptor


def _fsync_directory(path: Path) -> None:
    descriptor = _directory_descriptor(path, "durable state directory")
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _regular_file_bytes(
    path: Path,
    field: str,
    *,
    durable: bool = False,
    repair_mode: int | None = None,
    missing_ok: bool = False,
) -> bytes | None:
    path = _without_symlink_components(path, field)
    flags = os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        if missing_ok:
            return None
        raise CampaignError(f"{field} is missing") from None
    except OSError as exc:
        raise CampaignError(f"{field} is unreadable") from exc
    handle = None
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode):
            raise CampaignError(f"{field} must be a regular file")
        if repair_mode is not None and stat.S_IMODE(details.st_mode) != repair_mode:
            try:
                os.fchmod(descriptor, repair_mode)
            except OSError as exc:
                raise CampaignError(f"{field} mode is unsafe") from exc
        handle = os.fdopen(descriptor, "rb")
        descriptor = -1
        body = handle.read()
        if durable:
            os.fsync(handle.fileno())
        return body
    finally:
        if handle is not None:
            handle.close()
        elif descriptor >= 0:
            os.close(descriptor)


def _durable_evidence_bytes(path: Path, field: str) -> bytes:
    body = _regular_file_bytes(
        path,
        field,
        durable=True,
        repair_mode=_EVIDENCE_FILE_MODE,
    )
    assert body is not None
    _fsync_directory(path.parent)
    _fsync_directory(path.parent.parent)
    return body


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
    path = Path(unquote(parsed.path))
    if not path.is_absolute():
        raise CampaignError(f"{field} must be an absolute local file URI")
    return _assert_in_configured_state_root(path, field)


def _read_evidence(
    ref: object, *, field: str, kind: str | None = None
) -> tuple[dict[str, object], Path]:
    exact_ref = _artifact_ref(ref, field)
    path = _local_path(exact_ref["uri"], f"{field}.uri")
    try:
        body = _durable_evidence_bytes(path, field)
        payload = json.loads(body.decode("utf-8"))
    except (CampaignError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CampaignError(f"{field} is unreadable or malformed") from exc
    if hashlib.sha256(body).hexdigest() != exact_ref["sha256"]:
        raise CampaignError(f"{field} hash mismatch")
    if not isinstance(payload, dict) or _canonical_bytes(payload) != body:
        raise CampaignError(f"{field} is not canonical JSON")
    if kind is not None and payload.get("kind") != kind:
        raise CampaignError(f"{field} kind must be {kind}")
    return payload, path


def _path_ref(path: Path) -> dict[str, str]:
    resolved = _assert_in_configured_state_root(path, "campaign evidence")
    try:
        body = _regular_file_bytes(resolved, "campaign evidence")
    except (CampaignError, OSError) as exc:
        raise CampaignError("campaign evidence is unreadable") from exc
    assert body is not None
    return {"uri": resolved.as_uri(), "sha256": hashlib.sha256(body).hexdigest()}


def _validate_claim_payload(
    claim: Mapping[str, object], *, expected_ledger_path: Path | None = None
) -> tuple[CampaignLedger, Path, object]:
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
    expected_ref = {
        "uri": ledger_path.resolve().as_uri(),
        "sha256": hashlib.sha256(_canonical_bytes(claim["ledger"])).hexdigest(),
    }
    if ledger_ref != expected_ref:
        raise CampaignError("claim ledger reference is malformed")
    if (
        expected_ledger_path is not None
        and ledger_path.resolve() != expected_ledger_path.resolve()
    ):
        raise CampaignError("claim evidence belongs to another ledger")
    return claimed_ledger, ledger_path, attempt


def _claim_state(
    claim_ref: object,
) -> tuple[dict[str, object], CampaignLedger, Path, object]:
    exact_ref = _artifact_ref(claim_ref, "claim_ref")
    claim, claim_path = _read_evidence(
        exact_ref,
        field="claim_ref",
        kind=_CLAIM_KIND,
    )
    claimed_ledger, ledger_path, attempt = _validate_claim_payload(claim)
    expected_path = _claim_marker_path(ledger_path, attempt.attempt_id)
    if exact_ref["uri"] != expected_path.as_uri() or claim_path != expected_path:
        raise CampaignError("claim_ref must identify the canonical claim marker")
    return claim, claimed_ledger, ledger_path, attempt


def _evidence_dir(ledger_path: Path) -> Path:
    return _assert_in_configured_state_root(
        ledger_path.with_name(ledger_path.name + ".evidence"),
        "campaign evidence directory",
    )


def _attempt_marker_path(ledger_path: Path, kind: str, attempt_id: str) -> Path:
    safe_attempt_id = _required(attempt_id, "attempt_id")
    if "/" in safe_attempt_id or safe_attempt_id in {".", ".."}:
        raise CampaignError("attempt_id is unsafe")
    return _evidence_dir(ledger_path) / f"{kind}-{safe_attempt_id}.json"


def _claim_marker_path(ledger_path: Path, attempt_id: str) -> Path:
    return _attempt_marker_path(ledger_path, _CLAIM_KIND, attempt_id)


def _persist_immutable_at(
    path: Path, payload: Mapping[str, object]
) -> dict[str, str]:
    path = _assert_in_configured_state_root(path, "campaign evidence path")
    body = _canonical_bytes(payload)
    try:
        path.parent.mkdir(mode=_EVIDENCE_DIR_MODE)
    except FileExistsError:
        pass
    except OSError as exc:
        raise CampaignError("campaign evidence directory cannot be created") from exc
    try:
        parent_details = path.parent.lstat()
        if (
            not stat.S_ISDIR(parent_details.st_mode)
            or path.parent.is_symlink()
            or parent_details.st_uid not in {0, os.geteuid()}
            or parent_details.st_gid != 0
        ):
            raise OSError("unsafe evidence directory")
        if stat.S_IMODE(parent_details.st_mode) != _EVIDENCE_DIR_MODE:
            if parent_details.st_uid != os.geteuid():
                raise OSError("shared evidence directory mode drifted")
            os.chmod(path.parent, _EVIDENCE_DIR_MODE)
    except OSError as exc:
        raise CampaignError("campaign evidence directory is unsafe") from exc
    # Persist both the evidence directory metadata and its entry in the state
    # root before any marker is allowed to become a ledger predecessor.
    _fsync_directory(path.parent)
    _fsync_directory(path.parent.parent)

    try:
        path.lstat()
    except FileNotFoundError:
        pass
    else:
        existing = _durable_evidence_bytes(path, "immutable campaign evidence")
        if existing != body:
            raise CampaignError("immutable campaign evidence conflicts")
        return {
            "uri": path.resolve().as_uri(),
            "sha256": hashlib.sha256(body).hexdigest(),
        }

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fchmod(handle.fileno(), _EVIDENCE_FILE_MODE)
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            temporary.unlink()
            temporary = None
            existing = _durable_evidence_bytes(
                path, "immutable campaign evidence"
            )
            if existing != body:
                raise CampaignError("immutable campaign evidence conflicts")
        else:
            temporary.unlink()
            temporary = None
            _persistence_boundary("evidence_link")
            _fsync_directory(path.parent)
            _fsync_directory(path.parent.parent)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
    return {"uri": path.resolve().as_uri(), "sha256": hashlib.sha256(body).hexdigest()}


def _assert_claim_matches_current(
    claim: Mapping[str, object], ledger: CampaignLedger
) -> None:
    if ledger.to_dict() != claim["ledger"]:
        raise CampaignError("claim ledger changed after claim")


def _finish_marker_path(ledger_path: Path, attempt_id: str) -> Path:
    return _attempt_marker_path(ledger_path, _FINISH_KIND, attempt_id)


def _consumption_marker_path(ledger_path: Path, attempt_id: str) -> Path:
    return _attempt_marker_path(ledger_path, _CONSUMPTION_KIND, attempt_id)


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
        or payload.get("kind") != _CONSUMPTION_KIND
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
        or payload.get("kind") != _FINISH_KIND
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


def _finish_evidence_inventory(
    ledger_path: Path, ledger: CampaignLedger
) -> dict[str, tuple[dict[str, object], CampaignLedger, object, dict[str, str]]]:
    directory = _evidence_dir(ledger_path)
    if not directory.exists():
        return {}
    try:
        details = directory.lstat()
    except OSError as exc:
        raise CampaignError("campaign evidence directory is unreadable") from exc
    if not stat.S_ISDIR(details.st_mode):
        raise CampaignError("campaign evidence directory is unsafe")

    known_attempt_ids = {attempt.attempt_id for attempt in ledger.attempts}
    inventory = {}
    for evidence_path in sorted(directory.glob(f"{_FINISH_KIND}-*.json")):
        evidence_ref = _path_ref(evidence_path)
        finish, _ = _read_evidence(
            evidence_ref,
            field="finish history evidence",
            kind=_FINISH_KIND,
        )
        raw_attempt = finish.get("attempt")
        if not isinstance(raw_attempt, Mapping):
            raise CampaignError("campaign finish evidence schema is malformed")
        attempt_id = _required(raw_attempt.get("attempt_id"), "finish attempt_id")
        terminal_ledger, terminal_attempt = _validate_finish_payload(
            finish,
            ledger_path=ledger_path,
            attempt_id=attempt_id,
        )
        if evidence_path != _finish_marker_path(ledger_path, attempt_id):
            raise CampaignError("campaign finish evidence history is ambiguous")
        if attempt_id not in known_attempt_ids:
            raise CampaignError("campaign finish evidence has an unexplained history")
        if attempt_id in inventory:
            raise CampaignError("campaign finish evidence history is ambiguous")
        inventory[attempt_id] = (
            finish,
            terminal_ledger,
            terminal_attempt,
            evidence_ref,
        )
    return inventory


def _validate_terminal_evidence_history(
    ledger_path: Path, ledger: CampaignLedger
) -> None:
    ledger_value = ledger.to_dict()
    finish_inventory = _finish_evidence_inventory(ledger_path, ledger)
    for index, attempt in enumerate(ledger.attempts):
        if attempt.status == "active":
            continue
        candidate = finish_inventory.get(attempt.attempt_id)
        if candidate is None:
            raise CampaignError("terminal ledger is missing immutable finish evidence")
        _, terminal_ledger, _, _ = candidate
        expected = {
            **ledger_value,
            "attempts": ledger_value["attempts"][: index + 1],
        }
        if terminal_ledger.to_dict() != expected:
            raise CampaignError("immutable finish evidence history differs from ledger")


@contextmanager
def _locked_ledger(path: Path) -> Iterator[CampaignLedger]:
    path = _assert_in_configured_state_root(path, "ledger_path")
    descriptor = _directory_descriptor(path.parent, "canary ledger directory")
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        body = _regular_file_bytes(
            path,
            "persisted canary campaign ledger",
            durable=True,
            repair_mode=_LEDGER_MODE,
            missing_ok=True,
        )
        if body is not None:
            try:
                payload = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise CampaignError("persisted canary campaign ledger is malformed") from exc
            if _canonical_bytes(payload) != body:
                raise CampaignError(
                    "persisted canary campaign ledger is not canonical JSON"
                )
            ledger = CampaignLedger.from_dict(payload)
            _fsync_directory(path.parent)
        else:
            ledger = CampaignLedger.empty()
        _validate_terminal_evidence_history(path, ledger)
        yield ledger
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _persist(path: Path, ledger: CampaignLedger) -> dict[str, str]:
    path = _assert_in_configured_state_root(path, "ledger_path")
    try:
        details = path.lstat()
    except FileNotFoundError:
        pass
    else:
        if not stat.S_ISREG(details.st_mode):
            raise CampaignError("persisted canary campaign ledger must be a regular file")
    body = _canonical_bytes(ledger.to_dict())
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        os.fchmod(descriptor, _LEDGER_MODE)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, _LEDGER_MODE)
        _fsync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return {"uri": path.resolve().as_uri(), "sha256": hashlib.sha256(body).hexdigest()}


def _ledger_ref(path: Path, ledger: CampaignLedger) -> dict[str, str]:
    body = _canonical_bytes(ledger.to_dict())
    return {
        "uri": path.resolve().as_uri(),
        "sha256": hashlib.sha256(body).hexdigest(),
    }


def _claim_payload(
    path: Path, ledger: CampaignLedger, attempt: object
) -> dict[str, object]:
    return {
        "kind": _CLAIM_KIND,
        "schema_version": 1,
        "ledger_ref": _ledger_ref(path, ledger),
        "ledger": ledger.to_dict(),
        "campaign": attempt.campaign.to_dict(),
        "attempt": attempt.to_dict(),
    }


def _attempt_transition_identity(attempt: object) -> dict[str, object]:
    value = attempt.to_dict()
    value.pop("status")
    value.pop("completed_at")
    value.pop("terminal_ref")
    return value


def _claim_evidence_candidates(
    ledger_path: Path,
) -> list[tuple[dict[str, object], CampaignLedger, object, dict[str, str]]]:
    directory = _evidence_dir(ledger_path)
    if not directory.exists():
        return []
    try:
        details = directory.lstat()
    except OSError as exc:
        raise CampaignError("campaign evidence directory is unreadable") from exc
    if not stat.S_ISDIR(details.st_mode) or directory.is_symlink():
        raise CampaignError("campaign evidence directory is unsafe")
    candidates = []
    for evidence_path in sorted(directory.glob(f"{_CLAIM_KIND}-*.json")):
        evidence_ref = _path_ref(evidence_path)
        claim, _ = _read_evidence(
            evidence_ref,
            field="claim recovery evidence",
            kind=_CLAIM_KIND,
        )
        candidate_ledger, _, attempt = _validate_claim_payload(
            claim, expected_ledger_path=ledger_path
        )
        if evidence_path.resolve() != _claim_marker_path(
            ledger_path, attempt.attempt_id
        ).resolve():
            raise CampaignError("campaign claim recovery evidence is ambiguous")
        candidates.append((claim, candidate_ledger, attempt, evidence_ref))
    return candidates


def _classify_claim_evidence(
    ledger_path: Path, current: CampaignLedger
) -> tuple[
    list[tuple[dict[str, object], CampaignLedger, object, dict[str, str]]],
    list[tuple[dict[str, object], CampaignLedger, object, dict[str, str]]],
]:
    current_attempts = current.attempts
    indexes = {
        attempt.attempt_id: index for index, attempt in enumerate(current_attempts)
    }
    settled = []
    pending = []
    observed_attempt_ids = set()
    for candidate in _claim_evidence_candidates(ledger_path):
        _, candidate_ledger, candidate_attempt, _ = candidate
        observed_attempt_ids.add(candidate_attempt.attempt_id)
        index = indexes.get(candidate_attempt.attempt_id)
        if index is not None:
            current_attempt = current_attempts[index]
            if (
                len(candidate_ledger.attempts) != index + 1
                or [item.to_dict() for item in candidate_ledger.attempts[:-1]]
                != [item.to_dict() for item in current_attempts[:index]]
                or _attempt_transition_identity(candidate_attempt)
                != _attempt_transition_identity(current_attempt)
            ):
                raise CampaignError("claim recovery history diverges from ledger")
            if current_attempt.status == "active":
                if candidate_ledger.to_dict() != current.to_dict():
                    raise CampaignError("active claim evidence differs from ledger")
                settled.append(candidate)
            continue
        if (
            len(candidate_ledger.attempts) == len(current_attempts) + 1
            and [item.to_dict() for item in candidate_ledger.attempts[:-1]]
            == [item.to_dict() for item in current_attempts]
        ):
            pending.append(candidate)
            continue
        raise CampaignError("claim recovery evidence has an unexplained history")
    if len(settled) > 1 or len(pending) > 1:
        raise CampaignError("campaign claim recovery evidence is ambiguous")
    if any(
        attempt.status != "active"
        and attempt.attempt_id not in observed_attempt_ids
        for attempt in current_attempts
    ):
        raise CampaignError("terminal ledger is missing immutable claim evidence")
    return settled, pending


def _claim_result_for_path(
    *,
    path: Path,
    ledger: CampaignLedger,
    attempt: object,
    claim_ref: Mapping[str, str],
    recovered: bool = False,
) -> dict[str, object]:
    result = {
        "guard_only": False,
        "campaign": attempt.campaign.to_dict(),
        "attempt": attempt.to_dict(),
        "ledger_ref": _ledger_ref(path, ledger),
        "claim_ref": dict(claim_ref),
    }
    if recovered:
        result.update({"operation": "claim", "recovered": True})
    return result


def _finish_result(
    *,
    attempt: object,
    successful: bool,
    terminal_ref: Mapping[str, str],
    finish_ref: Mapping[str, str],
    recovered: bool = False,
) -> dict[str, object]:
    finish, _ = _read_evidence(
        finish_ref, field="finish_ref", kind=_FINISH_KIND
    )
    result = {
        "campaign_id": attempt.campaign_id,
        "attempt_id": attempt.attempt_id,
        "status": "successful" if successful else "failed",
        "terminal_ref": dict(terminal_ref),
        "ledger_ref": _artifact_ref(finish["ledger_ref"], "finish ledger_ref"),
        "finish_ref": dict(finish_ref),
    }
    if recovered:
        result.update({"operation": "finish", "recovered": True})
    return result


def _terminal_follows_active(
    active_ledger: CampaignLedger, terminal_ledger: CampaignLedger
) -> bool:
    active = active_ledger.attempts
    terminal = terminal_ledger.attempts
    return (
        len(active) == len(terminal)
        and bool(active)
        and [item.to_dict() for item in active[:-1]]
        == [item.to_dict() for item in terminal[:-1]]
        and active[-1].status == "active"
        and terminal[-1].status in {"failed", "successful"}
        and _attempt_transition_identity(active[-1])
        == _attempt_transition_identity(terminal[-1])
    )


def recover_campaign_state(*, ledger_path: str | Path) -> dict[str, object]:
    """Converge one interrupted claim/finish transition from immutable evidence."""

    path = Path(ledger_path)
    with _locked_ledger(path) as ledger:
        settled, pending = _classify_claim_evidence(path, ledger)
        active = [item for item in ledger.attempts if item.status == "active"]
        if active:
            attempt = active[0]
            finish_path = _finish_marker_path(path, attempt.attempt_id)
            if finish_path.exists():
                if pending:
                    raise CampaignError(
                        "active ledger has impossible pending claim evidence"
                    )
                if not settled:
                    raise CampaignError(
                        "active claim evidence is missing; cannot recover finish"
                    )
                finish_ref = _path_ref(finish_path)
                finish, _ = _read_evidence(
                    finish_ref,
                    field="finish_ref",
                    kind=_FINISH_KIND,
                )
                terminal_ledger, terminal_attempt = _validate_finish_payload(
                    finish,
                    ledger_path=path,
                    attempt_id=attempt.attempt_id,
                )
                if not _terminal_follows_active(ledger, terminal_ledger):
                    raise CampaignError("finish recovery history differs from active ledger")
                persisted_ref = _persist(path, terminal_ledger)
                _persistence_boundary("finish_ledger")
                if persisted_ref != finish["ledger_ref"]:
                    raise CampaignError("campaign finish ledger reference drift")
                return _finish_result(
                    attempt=terminal_attempt,
                    successful=bool(finish["successful"]),
                    terminal_ref=finish["terminal_ref"],
                    finish_ref=finish_ref,
                    recovered=True,
                )
            if pending:
                raise CampaignError("active ledger has impossible pending claim evidence")
            if settled:
                _, claimed_ledger, claimed_attempt, claim_ref = settled[0]
                return _claim_result_for_path(
                    path=path,
                    ledger=claimed_ledger,
                    attempt=claimed_attempt,
                    claim_ref=claim_ref,
                    recovered=True,
                )

            # Compatibility repair for the previous ledger-first writer. The
            # active bytes already contain the complete deterministic claim;
            # materialize that evidence once before allowing admission.
            claim_ref = _persist_immutable_at(
                _claim_marker_path(path, attempt.attempt_id),
                _claim_payload(path, ledger, attempt),
            )
            _persistence_boundary("claim_evidence")
            return _claim_result_for_path(
                path=path,
                ledger=ledger,
                attempt=attempt,
                claim_ref=claim_ref,
                recovered=True,
            )

        if settled:
            raise CampaignError("terminal ledger unexpectedly matches active claim evidence")
        if pending:
            _, claimed_ledger, claimed_attempt, claim_ref = pending[0]
            persisted_ref = _persist(path, claimed_ledger)
            _persistence_boundary("claim_ledger")
            claimed_ledger_ref = _ledger_ref(path, claimed_ledger)
            if persisted_ref != claimed_ledger_ref:
                raise CampaignError("campaign claim ledger persistence drift")
            return _claim_result_for_path(
                path=path,
                ledger=claimed_ledger,
                attempt=claimed_attempt,
                claim_ref=claim_ref,
                recovered=True,
            )

        if ledger.attempts:
            attempt = ledger.attempts[-1]
            finish_path = _finish_marker_path(path, attempt.attempt_id)
            if not finish_path.exists():
                raise CampaignError("terminal ledger is missing immutable finish evidence")
            finish_ref = _path_ref(finish_path)
            finish, _ = _read_evidence(
                finish_ref,
                field="finish_ref",
                kind=_FINISH_KIND,
            )
            terminal_ledger, terminal_attempt = _validate_finish_payload(
                finish,
                ledger_path=path,
                attempt_id=attempt.attempt_id,
            )
            if terminal_ledger.to_dict() != ledger.to_dict():
                raise CampaignError("terminal finish evidence differs from ledger")
            return _finish_result(
                attempt=terminal_attempt,
                successful=bool(finish["successful"]),
                terminal_ref=finish["terminal_ref"],
                finish_ref=finish_ref,
                recovered=True,
            )
        raise CampaignError("no interrupted canary transition to recover")


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
            marker_ref = _path_ref(marker_path)
            marker, _ = _read_evidence(
                marker_ref,
                field="consumption_ref",
                kind=_CONSUMPTION_KIND,
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
            "kind": _CONSUMPTION_KIND,
            "schema_version": 1,
            **binding,
            "campaign_id": current_attempt.campaign_id,
            "attempt_id": current_attempt.attempt_id,
            "ordinal": current_attempt.ordinal,
            "consumed_at": consumed_at.isoformat(),
        }
        marker_ref = _persist_immutable_at(marker_path, payload)
        _persistence_boundary("consumption_evidence")
        return marker_ref


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
            finish_ref = _path_ref(finish_path)
            finish, _ = _read_evidence(
                finish_ref,
                field="finish_ref",
                kind=_FINISH_KIND,
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
            kind=_CONSUMPTION_KIND,
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
        _, pending = _classify_claim_evidence(path, ledger)
        if pending:
            raise CampaignError(
                "interrupted canary claim requires recover before another claim"
            )
        attempt = ledger.claim(
            identity,
            now=now or datetime.now(UTC),
            guard_only=guard_only,
            predecessor_failure_ref=predecessor_failure_ref,
            remediation=remediation,
        )
        if guard_only:
            ledger_ref = None
            claim_ref = None
        else:
            claim_ref = _persist_immutable_at(
                _claim_marker_path(path, attempt.attempt_id),
                _claim_payload(path, ledger, attempt),
            )
            _persistence_boundary("claim_evidence")
            ledger_ref = _persist(path, ledger)
            _persistence_boundary("claim_ledger")
            if ledger_ref != _ledger_ref(path, ledger):
                raise CampaignError("campaign claim ledger persistence drift")
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

    if type(successful) is not bool:
        raise CampaignError("successful must be boolean")
    exact_terminal_ref = _artifact_ref(terminal_ref, "terminal_ref")
    path = Path(ledger_path)
    with _locked_ledger(path) as ledger:
        settled, pending = _classify_claim_evidence(path, ledger)
        if pending:
            raise CampaignError("claim recovery must finish before campaign finish")
        finish_path = _finish_marker_path(path, attempt_id)
        if finish_path.exists():
            if any(item.status == "active" for item in ledger.attempts) and not settled:
                raise CampaignError(
                    "active claim evidence is missing; cannot recover finish"
                )
            existing_ref = _path_ref(finish_path)
            existing, _ = _read_evidence(
                existing_ref,
                field="finish_ref",
                kind=_FINISH_KIND,
            )
            terminal_ledger, terminal_attempt = _validate_finish_payload(
                existing,
                ledger_path=path,
                attempt_id=attempt_id,
            )
            if (
                existing["successful"] is not successful
                or existing.get("terminal_ref") != exact_terminal_ref
            ):
                raise CampaignError("immutable campaign finish evidence conflicts")
            if ledger.to_dict() == terminal_ledger.to_dict():
                ledger_ref = _ledger_ref(path, terminal_ledger)
            else:
                if not _terminal_follows_active(ledger, terminal_ledger):
                    raise CampaignError("campaign finish recovery history differs")
                ledger_ref = _persist(path, terminal_ledger)
                _persistence_boundary("finish_ledger")
            if ledger_ref != existing["ledger_ref"]:
                raise CampaignError("campaign finish ledger reference drift")
            return _finish_result(
                attempt=terminal_attempt,
                successful=successful,
                terminal_ref=exact_terminal_ref,
                finish_ref=existing_ref,
            )
        active = [item for item in ledger.attempts if item.status == "active"]
        if len(active) != 1 or active[0].attempt_id != attempt_id:
            raise CampaignError("active canary attempt identity mismatch")
        attempt = active[0]
        if not settled:
            raise CampaignError("active claim evidence is missing; recover first")
        if successful:
            ledger.succeed(
                attempt,
                success_receipt_ref=exact_terminal_ref,
                now=now or datetime.now(UTC),
            )
        else:
            ledger.fail(
                attempt,
                failure_ref=exact_terminal_ref,
                now=now or datetime.now(UTC),
            )
        terminal_attempt = ledger.attempts[-1]
        ledger_ref = _ledger_ref(path, ledger)
        finish_ref = _persist_immutable_at(
            finish_path,
            {
                "kind": _FINISH_KIND,
                "schema_version": 1,
                "ledger_ref": ledger_ref,
                "ledger": ledger.to_dict(),
                "campaign": terminal_attempt.campaign.to_dict(),
                "attempt": terminal_attempt.to_dict(),
                "successful": successful,
                "terminal_ref": exact_terminal_ref,
            },
        )
        _persistence_boundary("finish_evidence")
        persisted_ref = _persist(path, ledger)
        _persistence_boundary("finish_ledger")
        if persisted_ref != ledger_ref:
            raise CampaignError("campaign finish ledger persistence drift")
    return _finish_result(
        attempt=terminal_attempt,
        successful=successful,
        terminal_ref=exact_terminal_ref,
        finish_ref=finish_ref,
    )


def _load_target_scope_ids(path: Path) -> tuple[str, ...]:
    if not path.is_absolute():
        raise CampaignError("target scopes path must be absolute")
    try:
        details = path.lstat()
        if not stat.S_ISREG(details.st_mode) or path.is_symlink():
            raise OSError("target scope input is not a regular file")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CampaignError("target scopes are unreadable or malformed") from exc
    if isinstance(value, Mapping):
        value = value.get("target_scope_ids")
    if not isinstance(value, list) or any(type(item) is not str for item in value):
        raise CampaignError("target scopes must be a JSON array of scope IDs")
    return tuple(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    claim = commands.add_parser(
        "claim", help="guard or durably claim one exact all-181 attempt"
    )
    claim.add_argument("--ledger-path", required=True)
    claim.add_argument("--release-commit", required=True)
    claim.add_argument("--release-tree-sha256", required=True)
    claim.add_argument("--registry-signature", required=True)
    claim.add_argument("--target-scopes", type=Path, required=True)
    claim.add_argument("--guard-only", action="store_true")
    claim.add_argument("--predecessor-failure-uri")
    claim.add_argument("--predecessor-failure-sha256")
    claim.add_argument("--remediation")

    finish = commands.add_parser(
        "finish", help="seal the terminal receipt for one active attempt"
    )
    finish.add_argument("--ledger-path", required=True)
    finish.add_argument("--attempt-id", required=True)
    finish.add_argument("--terminal-uri", required=True)
    finish.add_argument("--terminal-sha256", required=True)
    outcome = finish.add_mutually_exclusive_group(required=True)
    outcome.add_argument("--successful", action="store_true")
    outcome.add_argument("--failed", action="store_true")

    recover = commands.add_parser(
        "recover", help="converge an interrupted evidence/ledger transition"
    )
    recover.add_argument("--ledger-path", required=True)
    return parser


def _predecessor_ref(args: argparse.Namespace) -> dict[str, str] | None:
    values = (args.predecessor_failure_uri, args.predecessor_failure_sha256)
    if values == (None, None):
        return None
    if any(value is None for value in values):
        raise CampaignError(
            "predecessor failure URI and SHA-256 must be supplied together"
        )
    return {"uri": values[0], "sha256": values[1]}


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        ledger_path = _cli_ledger_path(args.ledger_path)
        if args.command == "claim":
            result = claim_campaign_attempt(
                ledger_path=ledger_path,
                release_commit=args.release_commit,
                release_tree_sha256=args.release_tree_sha256,
                registry_signature=args.registry_signature,
                target_scope_ids=_load_target_scope_ids(args.target_scopes),
                guard_only=args.guard_only,
                predecessor_failure_ref=_predecessor_ref(args),
                remediation=args.remediation,
            )
        elif args.command == "finish":
            result = finish_campaign_attempt(
                ledger_path=ledger_path,
                attempt_id=args.attempt_id,
                terminal_ref={
                    "uri": args.terminal_uri,
                    "sha256": args.terminal_sha256,
                },
                successful=args.successful,
            )
        else:
            result = recover_campaign_state(ledger_path=ledger_path)
    except CampaignError as exc:
        print(f"ESPN canary campaign refused: {exc}", file=sys.stderr)
        return 2
    except OSError:
        print(
            "ESPN canary campaign state I/O failed; inspect immutable evidence "
            "and run recover before retrying a mutating command",
            file=sys.stderr,
        )
        return 2
    sys.stdout.buffer.write(_canonical_bytes(result))
    return 0


__all__ = [
    "CANARY_STATE_ROOT_ENV",
    "DEFAULT_CANARY_LEDGER_PATH",
    "DEFAULT_CANARY_STATE_ROOT",
    "EXACT_CANARY_SCOPE_COUNT",
    "build_parser",
    "claim_campaign_attempt",
    "consume_campaign_claim",
    "finish_campaign_attempt",
    "main",
    "recover_campaign_state",
    "validate_campaign_consumption",
]


if __name__ == "__main__":
    raise SystemExit(main())
