#!/usr/bin/env python3
"""Build the immutable owner-approved SofaScore all-men campaign snapshot.

The large 4-August source recount stays outside git.  This builder joins its
source identities and seasons to the exact 1,504-id owner scope and the E0
kind estimates, validates the pinned file hashes, and emits a deterministic
runtime snapshot.  Unknown source gender/age/team-level remains explicitly
pending; it is never silently promoted to capture eligibility.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Iterable, Mapping, Sequence


class SnapshotError(ValueError):
    """The campaign inputs cannot prove the exact approved scope."""


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def snapshot_digest(snapshot: Mapping[str, Any]) -> str:
    """Return the content digest with the self-referential id removed."""

    unsigned = dict(snapshot)
    unsigned.pop("snapshot_id", None)
    return _digest(unsigned)


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise SnapshotError(f"{label} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise SnapshotError(f"{label} must be a positive integer") from exc
    if parsed <= 0:
        raise SnapshotError(f"{label} must be a positive integer")
    return parsed


def _index_unique(
    rows: Iterable[Mapping[str, Any]],
    *,
    key: str,
    label: str,
) -> dict[int, Mapping[str, Any]]:
    indexed: dict[int, Mapping[str, Any]] = {}
    for offset, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise SnapshotError(f"{label}[{offset}] must be an object")
        source_id = _positive_int(row.get(key), f"{label}[{offset}].{key}")
        if source_id in indexed:
            singular = label.removesuffix("s")
            raise SnapshotError(f"duplicate {singular} id {source_id}")
        indexed[source_id] = row
    return indexed


_BLOCKED_CLASSIFICATION = {
    "gender": frozenset({"female", "mixed", "women"}),
    "age_group": frozenset({"youth", "academy", "u21", "u23"}),
    "team_level": frozenset({"reserve", "academy", "youth"}),
}

_NEGATIVE_MARKERS = (
    (re.compile(
        r"(?:^|[-_ /])(?:women|womens|female|ladies|f[ée]minin(?:e)?|"
        r"femenin[ao]|femminile|frauen)(?:$|[-_ /])",
        re.I,
    ),
     "women marker in source identity"),
    (re.compile(r"(?:^|[-_ /])(?:u-?\d{1,2}|under-?\d{1,2}|youth|junior|primavera)(?:$|[-_ /])", re.I),
     "youth marker in source identity"),
    (re.compile(r"(?:^|[-_ /])(?:reserve|reserves|b-team|second-team|jong)(?:$|[-_ /])", re.I),
     "reserve marker in source identity"),
    (re.compile(r"(?:^|[-_ /])amateur(?:$|[-_ /])", re.I),
     "amateur marker in source identity"),
    (re.compile(r"(?:^|[-_ /])(?:futsal|beach-soccer|esports|e-sports)(?:$|[-_ /])", re.I),
     "non-field-football marker in source identity"),
)


def _candidate_ids_digest(values: Iterable[int]) -> str:
    return hashlib.sha256(json.dumps(
        sorted(int(value) for value in values), separators=(",", ":")
    ).encode()).hexdigest()


def _candidate_exclusion_reasons(
    source_id: int,
    candidate: Mapping[str, Any],
    source: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if str(source.get("sport_slug") or "").casefold() != "football":
        reasons.append("sport is not football")
    category = source.get("category")
    category_name = (
        str(category.get("name") or "") if isinstance(category, Mapping) else ""
    )
    candidate_category = str(candidate.get("category") or "")
    identity = " ".join((
        str(source.get("name") or ""),
        str(source.get("slug") or ""),
        str(source.get("page_path") or ""),
        category_name,
        str(candidate.get("name") or ""),
        str(candidate.get("slug") or ""),
        candidate_category,
    ))
    for pattern, reason in _NEGATIVE_MARKERS:
        if pattern.search(identity):
            reasons.append(reason)

    classification = source.get("classification")
    if not isinstance(classification, Mapping):
        raise SnapshotError(f"candidate {source_id} has no classification object")
    if str(classification.get("status") or "").casefold() == "excluded":
        reasons.append("source classification is excluded")
    reasons.extend(
        str(reason).strip()
        for reason in classification.get("exclusion_reasons") or []
        if str(reason).strip()
    )
    for field, blocked in _BLOCKED_CLASSIFICATION.items():
        token = str(classification.get(field) or "unknown").casefold()
        if token in blocked:
            reasons.append(f"{field}={token}")
    return sorted(set(reasons))


def _season_start_year(season: Mapping[str, Any]) -> int:
    token = str(season.get("canonical_season") or "").strip()
    if token.isdigit() and len(token) == 4:
        value = int(token)
        if 1900 <= value <= 2099:
            return value
        if 0 <= value <= 9999:
            short = int(token[:2])
            return (1900 if short >= 50 else 2000) + short
    for field in ("year", "source_name", "name"):
        value = str(season.get(field) or "")
        digits = "".join(ch if ch.isdigit() else " " for ch in value).split()
        for item in digits:
            if len(item) == 4 and 1900 <= int(item) <= 2099:
                return int(item)
    return -1


def _snapshot_seasons(source_id: int, raw: object) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        raise SnapshotError(f"candidate {source_id} seasons must be a list")
    seen_ids: set[int] = set()
    seasons: list[dict[str, Any]] = []
    for offset, item in enumerate(raw):
        if not isinstance(item, Mapping):
            raise SnapshotError(f"candidate {source_id} season[{offset}] is not an object")
        season_id = _positive_int(
            item.get("season_id"), f"candidate {source_id} season[{offset}].season_id"
        )
        if season_id in seen_ids:
            raise SnapshotError(f"candidate {source_id} has duplicate season id {season_id}")
        seen_ids.add(season_id)
        canonical = item.get("canonical_season")
        canonical_token = str(canonical).strip() if canonical is not None else None
        team_count = item.get("team_count")
        if team_count is not None:
            team_count = _positive_int(
                team_count, f"candidate {source_id} season {season_id}.team_count"
            )
        seasons.append(
            {
                "source_season_id": season_id,
                "source_name": str(
                    item.get("source_name") or item.get("name") or ""
                ).strip(),
                "canonical_season": canonical_token or None,
                "season_format": str(
                    item.get("season_format") or item.get("format") or "unknown"
                ).strip(),
                "start_year": _season_start_year(item),
                "team_count": team_count,
                "metadata_status": "ready" if team_count is not None else "pending",
                "team_count_evidence": item.get("team_count_evidence"),
            }
        )
    return sorted(
        seasons,
        key=lambda item: (item["start_year"], item["source_season_id"]),
        reverse=True,
    )


def build_snapshot(
    candidates_document: Mapping[str, Any],
    recount_document: Mapping[str, Any],
    estimates_document: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    """Join and validate the exact owner-approved source-native campaign."""

    if not isinstance(candidates_document, Mapping):
        raise SnapshotError("candidates root must be an object")
    if not isinstance(recount_document, Mapping):
        raise SnapshotError("recount root must be an object")
    if not isinstance(policy, Mapping) or int(policy.get("schema_version", 0)) != 1:
        raise SnapshotError("campaign policy schema_version must be 1")
    candidate_rows = candidates_document.get("candidates")
    recount_rows = recount_document.get("tournaments")
    if not isinstance(candidate_rows, list):
        raise SnapshotError("candidates must contain a candidates list")
    if not isinstance(recount_rows, list):
        raise SnapshotError("recount must contain a tournaments list")
    if not isinstance(estimates_document, Sequence) or isinstance(
        estimates_document, (str, bytes)
    ):
        raise SnapshotError("estimates root must be a list")

    candidates = _index_unique(
        candidate_rows, key="id", label="candidates"
    )
    recount = _index_unique(
        recount_rows, key="unique_tournament_id", label="recount tournaments"
    )
    estimates = _index_unique(estimates_document, key="id", label="estimates")
    expected_count = _positive_int(policy.get("candidate_count"), "candidate_count")
    if len(candidates) != expected_count:
        raise SnapshotError(
            f"candidate count mismatch: expected {expected_count}, got {len(candidates)}"
        )
    declared_count = candidates_document.get("count")
    if declared_count is not None and int(declared_count) != len(candidates):
        raise SnapshotError("candidate document count does not match its rows")
    missing = sorted(set(candidates) - set(recount))
    if missing:
        raise SnapshotError(f"candidate ids missing from recount: {missing[:10]}")
    if set(estimates) != set(candidates):
        raise SnapshotError("estimate ids differ from the approved candidate ids")
    expected_ids_digest = str(policy.get("candidate_ids_sha256") or "")
    actual_ids_digest = _candidate_ids_digest(candidates)
    if expected_ids_digest and expected_ids_digest != actual_ids_digest:
        raise SnapshotError("candidate id set does not match campaign policy")

    allowed_kinds = {
        str(item).casefold() for item in policy.get("allowed_kinds", [])
    }
    tournaments: list[dict[str, Any]] = []
    for source_id in sorted(candidates):
        candidate = candidates[source_id]
        source = recount[source_id]
        estimate = estimates[source_id]
        exclusion_reasons = _candidate_exclusion_reasons(
            source_id, candidate, source
        )
        kind = str(estimate.get("type") or "").casefold()
        if kind not in allowed_kinds:
            raise SnapshotError(f"candidate {source_id} has disallowed kind {kind!r}")
        seasons = _snapshot_seasons(source_id, source.get("seasons"))
        estimated_count = int(estimate.get("seasons") or 0)
        if estimated_count != len(seasons):
            raise SnapshotError(
                f"candidate {source_id} season count mismatch: "
                f"estimate={estimated_count}, recount={len(seasons)}"
            )
        category = source.get("category")
        if not isinstance(category, Mapping):
            raise SnapshotError(f"candidate {source_id} has no category object")
        classification = source.get("classification")
        assert isinstance(classification, Mapping)
        metadata_status = "excluded" if exclusion_reasons else "pending"
        if exclusion_reasons:
            for season in seasons:
                season["metadata_status"] = "excluded"
        tournaments.append(
            {
                "unique_tournament_id": source_id,
                "capture_key": f"SS-{source_id}",
                "name": str(source.get("name") or candidate.get("name") or "").strip(),
                "slug": str(source.get("slug") or candidate.get("slug") or "").strip(),
                "page_path": str(source.get("page_path") or "").strip(),
                "category": {
                    "id": category.get("id"),
                    "name": str(category.get("name") or "").strip(),
                    "slug": str(category.get("slug") or "").strip(),
                },
                "kind": kind,
                "classification": {
                    "sport": classification.get("sport"),
                    "gender": classification.get("gender"),
                    "age_group": classification.get("age_group"),
                    "team_level": classification.get("team_level"),
                    "status": (
                        "excluded" if exclusion_reasons
                        else classification.get("status")
                    ),
                    "exclusion_reasons": exclusion_reasons,
                    "evidence": list(classification.get("evidence") or []),
                },
                "eligibility_review": {
                    "status": "excluded" if exclusion_reasons else "approved",
                    "confirmed": {
                        "age_group": "adult",
                        "team_level": "first_team",
                        "professional": True,
                    },
                    "reviewed_by": "owner-approved-all-mens-campaign",
                    "reviewed_at": "2026-08-21",
                    "exclusion_reasons": exclusion_reasons,
                },
                "metadata_status": metadata_status,
                "seasons": seasons,
            }
        )

    policy_id = _digest(policy)
    candidate_ids_sha256 = _candidate_ids_digest(candidates)
    snapshot: dict[str, Any] = {
        "schema_version": 1,
        "scope": str(policy.get("scope") or "").strip(),
        "policy_id": policy_id,
        "campaign_id": _digest({
            "policy_id": policy_id,
            "candidate_ids_sha256": candidate_ids_sha256,
        }),
        "candidate_ids_sha256": candidate_ids_sha256,
        "candidate_count": len(tournaments),
        "source_sha256": dict(policy.get("source_sha256") or {}),
        "tournaments": tournaments,
    }
    snapshot["snapshot_id"] = snapshot_digest(snapshot)
    return snapshot


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotError(f"cannot read JSON {path}: {exc}") from exc


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _verify_source_hashes(
    policy: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> None:
    expected = policy.get("source_sha256")
    if not isinstance(expected, Mapping):
        raise SnapshotError("campaign policy has no source_sha256 object")
    for label, path in paths.items():
        actual = _file_sha256(path)
        if expected.get(label) != actual:
            raise SnapshotError(
                f"{label} sha256 mismatch: expected {expected.get(label)!r}, got {actual}"
            )


def _render(snapshot: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(snapshot, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")


def _write_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--recount", type=Path, required=True)
    parser.add_argument("--estimates", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)

    try:
        policy = _read_json(args.policy)
        sources = {
            "candidates": args.candidates,
            "recount": args.recount,
            "estimates": args.estimates,
        }
        _verify_source_hashes(policy, sources)
        snapshot = build_snapshot(
            _read_json(args.candidates),
            _read_json(args.recount),
            _read_json(args.estimates),
            policy,
        )
        payload = _render(snapshot)
        if args.check:
            if args.output.exists() and args.output.read_bytes() != payload:
                raise SnapshotError(f"snapshot drift: {args.output}")
        else:
            _write_atomic(args.output, payload)
        print(
            json.dumps(
                {
                    "status": "valid",
                    "candidate_count": snapshot["candidate_count"],
                    "snapshot_id": snapshot["snapshot_id"],
                    "output": str(args.output),
                    "check": bool(args.check),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    except SnapshotError as exc:
        print(f"snapshot error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
