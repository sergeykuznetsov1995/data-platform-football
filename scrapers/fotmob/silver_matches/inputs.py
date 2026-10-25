"""Pin and validate exported Bronze inputs for the bounded match Silver release.

These files are immutable read receipts, not proof that their bytes were reread
from S3. The raw blob hash claim and the hash of exported payload text are
intentionally different fields. No current-table fallback or version selection
is performed here: the caller must supply one selected batch per context.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import urlsplit

_CSV_LOCK = threading.Lock()
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_INTEGER = re.compile(r"[1-9][0-9]*\Z")


class InputValidationError(ValueError):
    """The fixed input set cannot be trusted as one committed match snapshot."""


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _reject_constant(value: str) -> None:
    raise InputValidationError(f"Non-finite JSON constant: {value}")


def _finite_float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise InputValidationError("Out-of-range JSON floating-point value")
    return result


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise InputValidationError(f"Ambiguous duplicate JSON key: {key}")
        result[key] = value
    return result


def _json(text: str, label: str) -> Any:
    try:
        return json.loads(text, object_pairs_hook=_object, parse_constant=_reject_constant, parse_float=_finite_float)
    except (ValueError, TypeError) as exc:
        raise InputValidationError(f"Invalid {label}: {exc}") from exc


def _positive_id(value: str, label: str) -> int:
    if not _INTEGER.fullmatch(value):
        raise InputValidationError(f"Invalid {label}: expected a positive integer")
    return int(value)


def _context(row: Sequence[str], offset: int = 0) -> tuple[int, str, int]:
    competition = _positive_id(row[offset], "competition_id")
    season = row[offset + 1]
    if not season or season != season.strip():
        raise InputValidationError("Invalid exact source_season_key")
    match = _positive_id(row[offset + 2], "match_id")
    return competition, season, match


def _rows(data: bytes, width: int, label: str) -> list[tuple[str, ...]]:
    try:
        text = data.decode("utf-8-sig")
        # csv exposes a process-global limit. Restore it even on malformed input;
        # the lock serializes this module's independent concurrent loaders.
        with _CSV_LOCK:
            previous = csv.field_size_limit()
            try:
                csv.field_size_limit(max(previous, len(text)))
                rows = [tuple(row) for row in csv.reader(io.StringIO(text, newline=""), strict=True)]
            finally:
                csv.field_size_limit(previous)
    except (UnicodeError, csv.Error) as exc:
        raise InputValidationError(f"Invalid {label} CSV") from exc
    if not rows:
        raise InputValidationError(f"Empty {label} CSV")
    for number, row in enumerate(rows, start=1):
        if len(row) != width:
            raise InputValidationError(f"{label} row {number}: expected {width} columns, got {len(row)}")
    return rows


@dataclass(frozen=True)
class MatchInput:
    """An immutable payload export and all equivalent committed observations."""

    record: tuple[str, ...]
    manifest_records: tuple[tuple[str, ...], ...]
    payload_text_sha256: str

    @property
    def context(self) -> tuple[int, str, int]:
        return int(self.record[0]), self.record[1], int(self.record[2])

    @property
    def context_key(self) -> str:
        return json.dumps(self.context, ensure_ascii=False, separators=(",", ":"))

    @property
    def source_batch_id(self) -> str:
        return self.record[3]

    @property
    def raw_payload_sha256_claim(self) -> str:
        return self.record[5]

    @property
    def payload_json(self) -> str:
        return self.record[10]

    @property
    def observed_at_raw(self) -> str:
        """Unmodified source timestamp; it does not imply a UTC timezone."""
        return self.record[4]


@dataclass(frozen=True)
class FrozenInputSet:
    inputs: tuple[MatchInput, ...]
    fingerprint: str
    payload_csv_sha256: str
    manifest_csv_sha256: str

    @property
    def records(self) -> tuple[tuple[str, ...], ...]:
        return tuple(item.record for item in self.inputs)

    def receipt(self) -> dict[str, Any]:
        return {
            "input_set_sha256": self.fingerprint,
            "payload_csv_sha256": self.payload_csv_sha256,
            "manifest_csv_sha256": self.manifest_csv_sha256,
            "matches": [
                {
                    "context": list(item.context),
                    "source_batch_id": item.source_batch_id,
                    "source_observed_at_raw": item.observed_at_raw,
                    "raw_payload_sha256_claim": item.raw_payload_sha256_claim,
                    "payload_text_sha256": item.payload_text_sha256,
                    "raw_uri": item.record[6],
                    "parser_version": item.record[7],
                    "manifest_observation_ids": sorted({r[16] for r in item.manifest_records if r[16]}),
                    "manifest_observation_count": len(item.manifest_records),
                }
                for item in self.inputs
            ],
            "scope": "Pinned CSV exports validated against manifest observations; raw S3 bytes not rehashed",
        }


def validate_input_bytes(payload_csv: bytes, manifest_csv: bytes) -> FrozenInputSet:
    """Validate complete selected batches without consulting changing sources.

    Extra historical manifests are allowed. For a selected context/batch every
    observed manifest must agree and be successful; a good row cannot conceal a
    stale, failed or conflicting observation for that same selected batch.
    """
    payloads = _rows(payload_csv, 11, "payload")
    manifests = _rows(manifest_csv, 17, "manifest")
    selected: dict[tuple[int, str, int], tuple[str, ...]] = {}
    for record in payloads:
        context = _context(record)
        if context in selected:
            raise InputValidationError(f"Duplicate selected match context: {context}")
        if not record[3] or not record[7] or not record[4]:
            raise InputValidationError(f"Missing batch/parser/observation identity: {context}")
        if not _HASH.fullmatch(record[5]):
            raise InputValidationError(f"Invalid raw payload hash claim: {context}")
        uri = urlsplit(record[6])
        if uri.scheme != "s3" or not uri.netloc or not uri.path:
            raise InputValidationError(f"Invalid raw payload URI: {context}")
        payload = _json(record[10], "payload_json")
        if not isinstance(payload, dict) or not payload:
            raise InputValidationError(f"Payload must be a nonempty object: {context}")
        available = _json(record[8], "available_sections_json")
        missing = _json(record[9], "missing_expected_sections_json")
        if not isinstance(available, dict) or any(type(v) is not bool for v in available.values()):
            raise InputValidationError(f"Invalid available sections: {context}")
        if missing != []:
            raise InputValidationError(f"Incomplete expected sections: {context}")
        selected[context] = record

    observations: dict[tuple[tuple[int, str, int], str], list[tuple[str, ...]]] = {}
    for manifest in manifests:
        if manifest[0] != "match":
            continue
        context = _context(manifest, 1)
        if context not in selected or manifest[4] != selected[context][3]:
            continue
        observations.setdefault((context, manifest[4]), []).append(manifest)

    validated: list[MatchInput] = []
    for context, record in sorted(selected.items()):
        matching = observations.get((context, record[3]), [])
        if not matching:
            raise InputValidationError(f"Orphan selected payload: {context}")
        expected_available = _json(record[8], "available_sections_json")
        committed_evidence = None
        for manifest in matching:
            if manifest[5:8] != ("success", "success", "false") or manifest[15]:
                raise InputValidationError(f"Uncommitted, stale or failed manifest: {context}")
            if (manifest[10], manifest[11], manifest[12]) != (record[5], record[6], record[7]):
                raise InputValidationError(f"Conflicting manifest content identity: {context}")
            if manifest[8] != record[4] or not manifest[9]:
                raise InputValidationError(f"Conflicting or incomplete observation timestamp: {context}")
            counts = _json(manifest[13], "manifest actual_counts_json")
            capabilities = _json(manifest[14], "manifest capabilities_json")
            if not isinstance(counts, dict) or type(counts.get("match_payloads")) is not int or counts["match_payloads"] != 1:
                raise InputValidationError(f"Incomplete match target batch: {context}")
            if not isinstance(capabilities, dict) or capabilities.get("missing_expected_sections") != []:
                raise InputValidationError(f"Incomplete manifest capabilities: {context}")
            manifest_available = capabilities.get("available_sections")
            if (not isinstance(manifest_available, dict)
                    or any(type(v) is not bool for v in manifest_available.values())
                    or manifest_available != expected_available):
                raise InputValidationError(f"Conflicting manifest available sections: {context}")
            evidence = json.dumps([counts, capabilities], sort_keys=True, ensure_ascii=False, separators=(",", ":"))
            if committed_evidence is not None and evidence != committed_evidence:
                raise InputValidationError(f"Conflicting manifest batch evidence: {context}")
            committed_evidence = evidence
        # Repeated equivalent observations remain evidence, never duplicate input rows.
        validated.append(MatchInput(record, tuple(sorted(set(matching))), _sha256(record[10].encode("utf-8"))))

    payload_hash = _sha256(payload_csv)
    manifest_hash = _sha256(manifest_csv)
    identity = {
        "format_version": 1,
        "payload_csv_sha256": payload_hash,
        "manifest_csv_sha256": manifest_hash,
        "selected": [
            [*item.context, item.source_batch_id, item.raw_payload_sha256_claim, item.payload_text_sha256]
            for item in validated
        ],
    }
    fingerprint = _sha256(json.dumps(identity, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    return FrozenInputSet(tuple(validated), fingerprint, payload_hash, manifest_hash)


def load_input_set(payload_csv: str | Path, manifest_csv: str | Path) -> FrozenInputSet:
    """Read each file once, then validate and operate solely on captured bytes."""
    return validate_input_bytes(Path(payload_csv).read_bytes(), Path(manifest_csv).read_bytes())
