"""Raw-first extraction of Transfermarkt achievement observations."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from types import MappingProxyType

from scrapers.transfermarkt.achievements import (
    ACHIEVEMENT_COLUMNS,
    ACHIEVEMENT_PARSER_REVISION,
    parse_achievements,
)
from scrapers.transfermarkt.raw_store import RawCaptureRecord


SCHEMA_REVISION = "1"
_LINEAGE_COLUMNS = (
    "observation_status", "source_capture_id", "raw_capture_id", "source_url",
    "source_body_hash", "source_endpoint", "scope_id", "cycle_id", "captured_at",
    "observed_at", "extracted_at", "run_id", "universe_version",
    "assessment_manifest_id", "parser_revision", "schema_revision",
)
ACHIEVEMENT_OBSERVATION_COLUMNS = tuple(dict.fromkeys((*ACHIEVEMENT_COLUMNS, *_LINEAGE_COLUMNS)))
ACHIEVEMENT_OBSERVATION_KEY = (
    "recipient_type", "recipient_id", "source_capture_id", "source_row_id",
)
_ENDPOINTS = {
    "player": "player_achievement",
    "club": "club_achievement",
    "coach": "coach_achievement",
}
_SLUG = re.compile(r"[a-zA-Z0-9]+(?:-[a-zA-Z0-9]+)*")
_DECIMAL = re.compile(r"[0-9]+")
_ISO_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})"
)


class AchievementObservationError(ValueError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _required_text(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AchievementObservationError(f"invalid {field}")
    return value


def _utc_iso(value: datetime | str, field: str) -> str:
    try:
        if isinstance(value, str) and not _ISO_TIMESTAMP.fullmatch(value):
            raise ValueError
        moment = datetime.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(moment, datetime) or moment.utcoffset() is None:
            raise ValueError
        return moment.astimezone(timezone.utc).isoformat()
    except (ValueError, OverflowError, TypeError) as exc:
        raise AchievementObservationError(f"invalid {field}") from exc


def _canonical(value: object) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class AchievementRecipient:
    recipient_type: str
    recipient_id: str | int
    slug: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.recipient_type, str) or self.recipient_type not in _ENDPOINTS:
            raise AchievementObservationError("invalid recipient_type")
        if isinstance(self.recipient_id, bool) or not isinstance(self.recipient_id, (str, int)):
            raise AchievementObservationError("invalid recipient_id")
        digits = str(self.recipient_id)
        if not _DECIMAL.fullmatch(digits) or not digits.lstrip("0"):
            raise AchievementObservationError("invalid recipient_id")
        object.__setattr__(self, "recipient_id", digits.lstrip("0"))
        if self.slug is None and self.recipient_type != "coach":
            return
        if not isinstance(self.slug, str) or not _SLUG.fullmatch(self.slug):
            raise AchievementObservationError("invalid slug")

    def inventory_entry(self) -> dict:
        return {
            "recipient_type": self.recipient_type,
            "recipient_id": self.recipient_id,
            "slug": self.slug,
        }


def achievement_url(
    *, recipient_type: str, recipient_id: str | int, slug: str | None = None,
) -> str:
    recipient = AchievementRecipient(recipient_type, recipient_id, slug)
    if recipient_type == "coach":
        return f"https://www.transfermarkt.com/{recipient.slug}/erfolge/trainer/{recipient.recipient_id}"
    return f"https://tmapi.transfermarkt.technology/{recipient_type}/{recipient.recipient_id}/achievement"


@dataclass(frozen=True)
class AchievementUniverse:
    universe_version: str
    recipients: tuple[AchievementRecipient, ...]
    assessment_manifest_id: str = field(init=False)

    def __post_init__(self) -> None:
        _required_text(self.universe_version, "universe_version")
        if not isinstance(self.recipients, (tuple, list)) or not self.recipients:
            raise AchievementObservationError("invalid recipients")
        seen = set()
        for recipient in self.recipients:
            if not isinstance(recipient, AchievementRecipient):
                raise AchievementObservationError("invalid recipient")
            identity = (recipient.recipient_type, recipient.recipient_id)
            if identity in seen:
                raise AchievementObservationError("duplicate recipient")
            seen.add(identity)
        ordered = sorted(self.recipients, key=lambda r: (r.recipient_type, r.recipient_id))
        object.__setattr__(self, "recipients", tuple(ordered))
        manifest = {
            "universe_version": self.universe_version,
            "recipients": [r.inventory_entry() for r in self.recipients],
        }
        object.__setattr__(self, "assessment_manifest_id", _digest(_canonical(manifest)))


@dataclass(frozen=True)
class AchievementExtraction:
    recipient: AchievementRecipient
    universe_version: str
    assessment_manifest_id: str
    run_id: str
    extracted_at: str
    raw_capture_id: str
    source_body_hash: str
    source_url: str
    source_endpoint: str
    scope_id: str
    cycle_id: str
    captured_at: str
    rows: tuple[Mapping[str, object], ...]

    def __post_init__(self) -> None:
        # Parser columns contain scalar values. Take a private copy before freezing.
        object.__setattr__(self, "rows", tuple(MappingProxyType(dict(row)) for row in self.rows))

    def lineage(self) -> dict:
        return {
            "recipient_type": self.recipient.recipient_type,
            "recipient_id": self.recipient.recipient_id,
            "source_capture_id": self.raw_capture_id,
            "raw_capture_id": self.raw_capture_id,
            "source_url": self.source_url,
            "source_body_hash": self.source_body_hash,
            "source_endpoint": self.source_endpoint,
            "scope_id": self.scope_id,
            "cycle_id": self.cycle_id,
            "captured_at": self.captured_at,
            "observed_at": self.captured_at,
            "extracted_at": self.extracted_at,
            "run_id": self.run_id,
            "universe_version": self.universe_version,
            "assessment_manifest_id": self.assessment_manifest_id,
            "parser_revision": ACHIEVEMENT_PARSER_REVISION,
            "schema_revision": SCHEMA_REVISION,
        }

    def canonical_json(self) -> str:
        return _canonical({
            **self.lineage(),
            "recipient": self.recipient.inventory_entry(),
            "rows": [dict(row) for row in self.rows],
        })

    def plan_sha256(self) -> str:
        return _digest(self.canonical_json())


def _check_capture(
    body: bytes, raw: RawCaptureRecord, recipient: AchievementRecipient,
) -> None:
    if not isinstance(body, bytes) or not isinstance(raw, RawCaptureRecord):
        raise AchievementObservationError("invalid raw capture")
    if raw.status_code != 200:
        raise AchievementObservationError("invalid status_code")
    if raw.endpoint != _ENDPOINTS[recipient.recipient_type]:
        raise AchievementObservationError("unexpected endpoint")
    if raw.url != achievement_url(**recipient.inventory_entry()):
        raise AchievementObservationError("unexpected url")
    expected_scope = f"achievement:{recipient.recipient_type}:{recipient.recipient_id}"
    if raw.scope_id != expected_scope:
        raise AchievementObservationError("unexpected scope_id")
    if raw.content_hash != hashlib.sha256(body).hexdigest():
        raise AchievementObservationError("content_hash mismatch")
    for name in ("capture_id", "scope_id", "cycle_id"):
        _required_text(getattr(raw, name), name)


def extract_achievement_capture(
    body: bytes, raw: RawCaptureRecord, *, recipient: AchievementRecipient,
    universe: AchievementUniverse, run_id: str, extracted_at: datetime | str,
) -> AchievementExtraction:
    if not isinstance(recipient, AchievementRecipient):
        raise AchievementObservationError("invalid recipient")
    if not isinstance(universe, AchievementUniverse) or recipient not in universe.recipients:
        raise AchievementObservationError("recipient outside universe")
    _required_text(run_id, "run_id")
    extracted = _utc_iso(extracted_at, "extracted_at")
    _check_capture(body, raw, recipient)
    captured = _utc_iso(raw.fetched_at, "fetched_at")
    try:
        text = body.decode("utf-8")
        payload = text if recipient.recipient_type == "coach" else json.loads(text)
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise AchievementObservationError("invalid achievement body") from exc

    result = AchievementExtraction(
        recipient=recipient,
        universe_version=universe.universe_version,
        assessment_manifest_id=universe.assessment_manifest_id,
        run_id=run_id,
        extracted_at=extracted,
        raw_capture_id=raw.capture_id,
        source_body_hash=raw.content_hash,
        source_url=raw.url,
        source_endpoint=raw.endpoint,
        scope_id=raw.scope_id,
        cycle_id=raw.cycle_id,
        captured_at=captured,
        rows=(),
    )
    observations = _parse_observations(payload, recipient)
    lineage = result.lineage()
    result = replace(result, rows=tuple({**row, **lineage} for row in observations))
    errors = validate_achievement_extraction(result)
    if errors:
        raise AchievementObservationError(errors[0])
    return result


def _parse_observations(
    payload: object,
    recipient: AchievementRecipient,
) -> list[dict[str, object]]:
    try:
        result = parse_achievements(
            payload,
            recipient_type=recipient.recipient_type,
            recipient_id=recipient.recipient_id,
        )
    except ValueError as exc:
        reason = getattr(exc, "reason", "achievement parse failed")
        raise AchievementObservationError(reason) from exc
    if result.status == "observed_empty":
        row = {column: None for column in ACHIEVEMENT_COLUMNS}
        row.update({
            "recipient_type": recipient.recipient_type,
            "recipient_id": recipient.recipient_id,
            "source_row_id": "recipient",
            "observation_status": "observed_empty",
        })
        return [row]
    rows = []
    for achievement in result.rows:
        row = achievement.as_dict()
        row["observation_status"] = "observed_present"
        rows.append(row)
    return rows


def validate_achievement_extraction(extraction: AchievementExtraction) -> list[str]:
    if not isinstance(extraction, AchievementExtraction):
        return ["invalid extraction"]
    if not isinstance(extraction.recipient, AchievementRecipient):
        return ["invalid recipient"]
    errors = []
    lineage = extraction.lineage()
    for name, value in lineage.items():
        if not isinstance(value, str) or not value.strip():
            errors.append(f"invalid {name}")
    for name in ("captured_at", "extracted_at"):
        try:
            if _utc_iso(lineage[name], name) != lineage[name]:
                errors.append(f"noncanonical {name}")
        except AchievementObservationError as exc:
            errors.append(exc.reason)
    for name in ("source_body_hash", "assessment_manifest_id"):
        if not isinstance(lineage[name], str) or not re.fullmatch(r"[0-9a-f]{64}", lineage[name]):
            errors.append(f"invalid {name}")
    if extraction.source_url != achievement_url(**extraction.recipient.inventory_entry()):
        errors.append("unexpected source_url")
    if extraction.source_endpoint != _ENDPOINTS[extraction.recipient.recipient_type]:
        errors.append("unexpected source_endpoint")
    if not extraction.rows:
        errors.append("missing observations")

    keys = set()
    for index, row in enumerate(extraction.rows):
        if row.keys() != set(ACHIEVEMENT_OBSERVATION_COLUMNS):
            errors.append(f"row {index}: incorrect columns")
        for name, expected in lineage.items():
            if row.get(name) != expected:
                errors.append(f"row {index}: unexpected {name}")
        key = tuple(row.get(name) for name in ACHIEVEMENT_OBSERVATION_KEY)
        if not all(isinstance(value, str) and value.strip() for value in key):
            errors.append(f"row {index}: invalid observation key")
        elif key in keys:
            errors.append(f"row {index}: duplicate observation key")
        else:
            keys.add(key)
        status = row.get("observation_status")
        if status == "observed_empty":
            if len(extraction.rows) != 1 or row.get("source_row_id") != "recipient":
                errors.append(f"row {index}: invalid empty observation")
        elif status == "observed_present":
            if row.get("source_row_id") == "recipient":
                errors.append(f"row {index}: invalid detail row ID")
        else:
            errors.append(f"row {index}: invalid observation_status")
    return errors
