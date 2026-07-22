"""Collect and classify immutable raw evidence for one backfill scope attempt."""

from __future__ import annotations

import json
import hashlib
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from scrapers.transfermarkt.raw_store import RawAttemptEnvelopeRecord
from utils.transfermarkt_backfill_state import AttemptOutcome


UNAVAILABLE_HTTP_STATUSES = frozenset({404, 410})


class BackfillAttemptEvidenceError(RuntimeError):
    """Attempt files or raw envelopes are inconsistent."""


@dataclass(frozen=True)
class ClassifiedScopeAttempt:
    outcome: AttemptOutcome
    raw_evidence_ids: tuple[str, ...]
    error_class: str | None
    error_message: str | None
    retry_after_seconds: int | None
    manifest_path: str | None
    checkpoint_path: str | None
    observed_at: datetime | None = None


def _load_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BackfillAttemptEvidenceError(f"attempt JSON is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise BackfillAttemptEvidenceError(f"attempt JSON is not an object: {path}")
    return value


def _matching_attempt_intents(
    entities: Path,
    *,
    campaign_id: str,
    child_cycle_id: str,
    scope_id: str,
    batch_id: str,
    claim_generation: int,
    attempt_sequence: int,
) -> tuple[dict[str, Any], ...]:
    expected = {
        "campaign_id": campaign_id,
        "child_cycle_id": child_cycle_id,
        "scope_id": scope_id,
        "batch_id": batch_id,
        "claim_generation": claim_generation,
        "attempt_sequence": attempt_sequence,
    }
    matches: list[dict[str, Any]] = []
    if not entities.is_dir():
        return ()
    for path in sorted(entities.glob(
        f".source-attempt-{claim_generation}-{attempt_sequence}-*.json"
    )):
        value = _load_object(path)
        if (
            value.get("contract_version") != 1
            or value.get("status") != "entered"
            or any(value.get(key) != item for key, item in expected.items())
        ):
            raise BackfillAttemptEvidenceError(
                f"source attempt intent identity drifted: {path}"
            )
        parser_entity = str(value.get("parser_entity") or "").strip()
        failure_path = Path(str(value.get("failure_path") or ""))
        if (
            not parser_entity
            or failure_path != entities / (
                f"{parser_entity}-failed-{claim_generation}-"
                f"{attempt_sequence}.json"
            )
        ):
            raise BackfillAttemptEvidenceError(
                f"source attempt intent path drifted: {path}"
            )
        matches.append(value)
    return tuple(matches)


def _envelope_ids(value: Mapping[str, Any]) -> tuple[str, ...]:
    attempts = value.get("raw_attempts", ())
    if attempts in (None, ""):
        attempts = ()
    if not isinstance(attempts, (list, tuple)):
        raise BackfillAttemptEvidenceError("raw_attempts must be a list")
    ids: list[str] = []
    for item in attempts:
        if not isinstance(item, Mapping):
            raise BackfillAttemptEvidenceError("raw attempt entry must be an object")
        envelope_id = str(item.get("envelope_id") or "").strip()
        if len(envelope_id) != 64 or any(ch not in "0123456789abcdef" for ch in envelope_id):
            raise BackfillAttemptEvidenceError("raw attempt envelope id is invalid")
        ids.append(envelope_id)
    return tuple(ids)


def has_matching_scope_attempt_result(
    *,
    result_base_dir: str,
    entity_dir: str,
    campaign_id: str,
    child_cycle_id: str,
    scope_id: str,
    batch_id: str,
    claim_generation: int,
    attempt_sequence: int,
) -> bool:
    """Return whether this exact physical attempt already reached a CLI result.

    The mutable filename is safe only because every identity field is checked.
    A stale status from an earlier batch/generation must never suppress a new
    attempt, while an exact failed status is a replay fence even when no raw
    envelope exists (that case is finalized as a platform incident).
    """

    base = Path(result_base_dir)
    entities = Path(entity_dir)
    if not base.is_absolute() or not entities.is_absolute():
        raise BackfillAttemptEvidenceError("attempt result paths must be absolute")
    try:
        entities.relative_to(base)
    except ValueError as exc:
        raise BackfillAttemptEvidenceError(
            "entity_dir escapes result_base_dir"
        ) from exc
    intents = _matching_attempt_intents(
        entities,
        campaign_id=str(campaign_id),
        child_cycle_id=str(child_cycle_id),
        scope_id=str(scope_id),
        batch_id=str(batch_id),
        claim_generation=int(claim_generation),
        attempt_sequence=int(attempt_sequence),
    )
    if intents:
        return True
    status_path = base / "scope-status.json"
    if not status_path.is_file():
        return False
    status = _load_object(status_path)
    expected = {
        "backfill_campaign_id": str(campaign_id),
        "backfill_batch_id": str(batch_id),
        "backfill_claim_generation": str(int(claim_generation)),
        "backfill_attempt_sequence": str(int(attempt_sequence)),
        "child_cycle_id": str(child_cycle_id),
        "scope_id": str(scope_id),
    }
    if status.get("status") not in {"complete", "failed"}:
        return False
    return all(str(status.get(key) or "") == value for key, value in expected.items())


def _retry_after_seconds(
    value: object,
    *,
    observed_at: datetime,
) -> int | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return max(0, int(raw))
    except ValueError:
        pass
    try:
        target = parsedate_to_datetime(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    if target.tzinfo is None or target.utcoffset() is None:
        target = target.replace(tzinfo=timezone.utc)
    seconds = (
        target.astimezone(timezone.utc) - observed_at.astimezone(timezone.utc)
    ).total_seconds()
    return max(0, int(math.ceil(seconds)))


def collect_scope_attempt_evidence(
    *,
    result_base_dir: str,
    entity_dir: str,
    scope_manifest_path: str,
    scope_id: str,
    raw_store: Any,
    campaign_id: str | None = None,
    child_cycle_id: str | None = None,
    batch_id: str | None = None,
    claim_generation: int | None = None,
    attempt_sequence: int | None = None,
) -> ClassifiedScopeAttempt:
    """Verify all entity envelopes and classify only from durable evidence."""

    base = Path(result_base_dir)
    entities = Path(entity_dir)
    manifest = Path(scope_manifest_path)
    if not base.is_absolute() or not entities.is_absolute() or not manifest.is_absolute():
        raise BackfillAttemptEvidenceError("attempt paths must be absolute")
    try:
        entities.relative_to(base)
        manifest.relative_to(base)
    except ValueError as exc:
        raise BackfillAttemptEvidenceError("attempt paths escape result_base_dir") from exc

    status_path = base / "scope-status.json"
    status_value = _load_object(status_path) if status_path.is_file() else None
    backfill_context = any(
        item is not None
        for item in (
            campaign_id,
            child_cycle_id,
            batch_id,
            claim_generation,
            attempt_sequence,
        )
    )
    if backfill_context:
        if (
            not campaign_id
            or not child_cycle_id
            or not batch_id
            or not isinstance(claim_generation, int)
            or claim_generation < 1
            or not isinstance(attempt_sequence, int)
            or attempt_sequence < 1
        ):
            raise BackfillAttemptEvidenceError("backfill attempt context is incomplete")
        context_matches = bool(
            status_value is not None
            and status_value.get("backfill_campaign_id") == campaign_id
            and status_value.get("backfill_batch_id") == batch_id
            and status_value.get("child_cycle_id") == child_cycle_id
            and status_value.get("scope_id") == scope_id
            and str(status_value.get("backfill_claim_generation") or "")
            == str(claim_generation)
            and str(status_value.get("backfill_attempt_sequence") or "")
            == str(attempt_sequence)
        )
        if not context_matches:
            status_value = None
        attempt_intents = _matching_attempt_intents(
            entities,
            campaign_id=str(campaign_id),
            child_cycle_id=str(child_cycle_id),
            scope_id=scope_id,
            batch_id=str(batch_id),
            claim_generation=claim_generation,
            attempt_sequence=attempt_sequence,
        )
    else:
        attempt_intents = ()
    result_files: list[Path] = []
    current_failure_files: list[Path] = []
    if entities.is_dir():
        result_files.extend(
            path for path in sorted(entities.glob("*.json"))
            if "-failed" not in path.stem
        )
        if backfill_context:
            current_failure_files = sorted(entities.glob(
                f"*-failed-{claim_generation}-{attempt_sequence}.json"
            ))
            result_files.extend(current_failure_files)
        else:
            current_failure_files = sorted(entities.glob("*-failed*.json"))
            result_files.extend(current_failure_files)
    if backfill_context and attempt_intents and current_failure_files:
        intended_failures = {
            Path(str(item["failure_path"])) for item in attempt_intents
        }
        if any(path not in intended_failures for path in current_failure_files):
            raise BackfillAttemptEvidenceError(
                "current failure artifact is not bound to its source intent"
            )
    ids: list[str] = []
    current_ids: list[str] = []
    errors: list[str] = []
    current_failure_values: list[Mapping[str, Any]] = []
    for path in result_files:
        value = _load_object(path)
        if path in current_failure_files:
            current_failure_values.append(value)
        value_ids = _envelope_ids(value)
        ids.extend(value_ids)
        if path in current_failure_files:
            current_ids.extend(value_ids)
        raw_errors = value.get("errors", ())
        if isinstance(raw_errors, (list, tuple)):
            errors.extend(str(item) for item in raw_errors if str(item).strip())
    unique_ids = tuple(dict.fromkeys(ids))
    records: list[RawAttemptEnvelopeRecord] = []
    for envelope_id in unique_ids:
        try:
            record = raw_store.verify_attempt_envelope(envelope_id)
        except Exception as exc:  # adapter boundary
            raise BackfillAttemptEvidenceError(
                f"raw attempt envelope cannot be verified: {envelope_id}"
            ) from exc
        if record.scope_id != scope_id:
            raise BackfillAttemptEvidenceError("raw attempt envelope scope mismatch")
        if child_cycle_id is not None and record.cycle_id != child_cycle_id:
            raise BackfillAttemptEvidenceError("raw attempt envelope cycle mismatch")
        records.append(record)
    by_envelope_id = {item.envelope_id: item for item in records}
    current_records = [
        by_envelope_id[item]
        for item in tuple(dict.fromkeys(current_ids))
    ]

    def observed(record: RawAttemptEnvelopeRecord) -> datetime:
        try:
            value = datetime.fromisoformat(record.observed_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise BackfillAttemptEvidenceError(
                "raw attempt observed_at is invalid"
            ) from exc
        if value.tzinfo is None or value.utcoffset() is None:
            raise BackfillAttemptEvidenceError("raw attempt observed_at lacks timezone")
        return value.astimezone(timezone.utc)

    latest = max(records, key=observed) if records else None
    observed_at = observed(latest) if latest is not None else None

    checkpoint = base / "scope-cycle-checkpoint.json"
    checkpoint_path = str(checkpoint) if checkpoint.is_file() else None
    manifest_is_current = manifest.is_file() and (
        not backfill_context
        or (status_value is not None and status_value.get("status") == "complete")
    )
    if manifest_is_current:
        if not records:
            raise BackfillAttemptEvidenceError(
                "captured scope has no immutable raw attempt envelopes"
            )
        return ClassifiedScopeAttempt(
            outcome=AttemptOutcome.CAPTURED,
            raw_evidence_ids=unique_ids,
            error_class=None,
            error_message=None,
            retry_after_seconds=None,
            manifest_path=str(manifest),
            checkpoint_path=checkpoint_path,
            observed_at=observed_at,
        )
    has_current_failure = bool(
        current_failure_files
        and (
            (status_value is not None and status_value.get("status") == "failed")
            or attempt_intents
        )
    ) if backfill_context else True
    if not records or not has_current_failure:
        return ClassifiedScopeAttempt(
            outcome=AttemptOutcome.PLATFORM_ERROR,
            raw_evidence_ids=(),
            error_class="missing_raw_attempt_evidence",
            error_message=(
                str(status_value.get("error") or "")[:500]
                if status_value is not None
                else (errors[-1][:500] if errors else None)
            ) or None,
            retry_after_seconds=None,
            manifest_path=None,
            checkpoint_path=checkpoint_path,
            observed_at=None,
        )
    classification_records = current_records if backfill_context else records
    if not classification_records:
        return ClassifiedScopeAttempt(
            outcome=AttemptOutcome.PLATFORM_ERROR,
            raw_evidence_ids=unique_ids,
            error_class="missing_current_raw_attempt_evidence",
            error_message=errors[-1][:500] if errors else None,
            retry_after_seconds=None,
            manifest_path=None,
            checkpoint_path=checkpoint_path,
            observed_at=None,
        )
    latest = max(classification_records, key=observed)
    observed_at = observed(latest)
    if backfill_context:
        if len(current_failure_values) != 1:
            return ClassifiedScopeAttempt(
                outcome=AttemptOutcome.PLATFORM_ERROR,
                raw_evidence_ids=unique_ids,
                error_class="ambiguous_failure_classification",
                error_message=None,
                retry_after_seconds=None,
                manifest_path=None,
                checkpoint_path=checkpoint_path,
                observed_at=observed_at,
            )
        failure = current_failure_values[0]
        failure_stage = str(failure.get("failure_stage") or "").strip()
        failure_kind = str(failure.get("failure_kind") or "").strip()
        try:
            network_fetches = int(failure.get("network_fetches", -1))
        except (TypeError, ValueError):
            network_fetches = -1
        if failure_stage not in {"source", "platform"} or not failure_kind:
            return ClassifiedScopeAttempt(
                outcome=AttemptOutcome.PLATFORM_ERROR,
                raw_evidence_ids=unique_ids,
                error_class="missing_failure_classification",
                error_message=errors[-1][:500] if errors else None,
                retry_after_seconds=None,
                manifest_path=None,
                checkpoint_path=checkpoint_path,
                observed_at=observed_at,
            )
        if failure_stage == "platform" or (
            latest.outcome_kind == "transport_error"
            and latest.error_kind == "proxy"
            and network_fetches == 0
        ):
            return ClassifiedScopeAttempt(
                outcome=AttemptOutcome.PLATFORM_ERROR,
                raw_evidence_ids=unique_ids,
                error_class=f"platform_{failure_kind}"[:200],
                error_message=errors[-1][:500] if errors else None,
                retry_after_seconds=None,
                manifest_path=None,
                checkpoint_path=checkpoint_path,
                observed_at=observed_at,
            )
    retry_after: int | None = None
    if latest.capture_id is not None:
        try:
            _, capture = raw_store.load_capture(latest.capture_id)
        except Exception as exc:  # adapter boundary
            raise BackfillAttemptEvidenceError("latest response capture is invalid") from exc
        raw_retry_after = capture.headers.get("retry-after")
        retry_after = _retry_after_seconds(
            raw_retry_after,
            observed_at=observed(latest),
        )

    if (
        latest.status_code in UNAVAILABLE_HTTP_STATUSES
        and latest.endpoint == "listing"
    ):
        outcome = AttemptOutcome.UNAVAILABLE_CONFIRMATION
        error_class = None
        error_message = None
    else:
        outcome = AttemptOutcome.SOURCE_ERROR
        if latest.outcome_kind == "transport_error":
            error_class = f"transport_{latest.error_kind or 'unknown'}"
        elif latest.status_code is not None:
            error_class = f"http_{latest.status_code}"
        else:
            error_class = "source_parse_or_schema_error"
        error_message = errors[-1][:500] if errors else None
    return ClassifiedScopeAttempt(
        outcome=outcome,
        raw_evidence_ids=unique_ids,
        error_class=error_class,
        error_message=error_message,
        retry_after_seconds=retry_after,
        manifest_path=None,
        checkpoint_path=checkpoint_path,
        observed_at=observed_at,
    )


def verify_envelope_set(
    raw_store: Any,
    envelope_ids: Iterable[str],
) -> tuple[RawAttemptEnvelopeRecord, ...]:
    """Read back one exact, non-duplicated immutable evidence set."""

    ids = tuple(envelope_ids)
    if len(ids) != len(set(ids)):
        raise BackfillAttemptEvidenceError("raw envelope evidence is duplicated")
    records = tuple(raw_store.verify_attempt_envelope(item) for item in ids)
    if tuple(item.envelope_id for item in records) != ids:
        raise BackfillAttemptEvidenceError("raw envelope evidence identity drift")
    for record in records:
        if record.capture_id is None:
            continue
        try:
            body, capture = raw_store.load_capture(record.capture_id)
        except Exception as exc:  # storage adapter boundary
            raise BackfillAttemptEvidenceError(
                f"raw envelope capture cannot be verified: {record.capture_id}"
            ) from exc
        if (
            capture.capture_id != record.capture_id
            or capture.content_hash != record.raw_body_hash
            or hashlib.sha256(body).hexdigest() != record.raw_body_hash
        ):
            raise BackfillAttemptEvidenceError(
                f"raw envelope capture identity drifted: {record.capture_id}"
            )
    return records


__all__ = [
    "BackfillAttemptEvidenceError",
    "ClassifiedScopeAttempt",
    "UNAVAILABLE_HTTP_STATUSES",
    "collect_scope_attempt_evidence",
    "has_matching_scope_attempt_result",
    "verify_envelope_set",
]
