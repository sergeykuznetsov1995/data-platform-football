#!/usr/bin/env python3
"""Atomically review and activate SofaScore registry tournaments.

The grammar is command-first (``<command> --tournament-ids ...``) so a single
compare-and-swap write can carry a whole onboarding wave.  Batch review is
all-or-nothing: every row is applied in memory first, so one ineligible
tournament aborts the wave before the registry file is touched.

Layer order is enforced by the registry helpers, not by this CLI: approval is
impossible while the *source* classification is not male football, and
activation is impossible without a canonical season.  A batch cannot skip the
machine layer.
"""

from __future__ import annotations

import argparse
import json
import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.sofascore.catalog import SofaScoreCatalog, registry_path  # noqa: E402
from scrapers.sofascore.discovery import write_registry_atomic  # noqa: E402
from scrapers.sofascore.registry import (  # noqa: E402
    REVIEW_EVIDENCE_TODO_KEY,
    ActivationError,
    approve_tournament,
    reject_tournament,
    set_activation,
)


COMPETITIONS_PATH = PROJECT_ROOT / "configs" / "medallion" / "competitions.yaml"
REVIEW_TODO = (
    "TODO: confirm adult, first-team participation with an out-of-source "
    "reference before approving"
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Review or activate SofaScore tournaments atomically",
    )
    parser.add_argument("--registry", default=str(registry_path()))
    commands = parser.add_subparsers(dest="command", required=True)

    approve = commands.add_parser("approve")
    approve.add_argument("--tournament-id", type=int, required=True)
    approve.add_argument("--canonical-id", required=True)
    approve.add_argument("--reviewed-by", required=True)
    approve.add_argument("--reviewed-at")
    approve.add_argument("--evidence", action="append", required=True)
    approve.add_argument("--notes")

    reject = commands.add_parser("reject")
    reject.add_argument("--tournament-id", type=int, required=True)
    reject.add_argument("--reviewed-by", required=True)
    reject.add_argument("--reviewed-at")
    reject.add_argument("--evidence", action="append", required=True)
    reject.add_argument("--notes")

    for name in ("enable", "disable"):
        activation = commands.add_parser(name)
        activation.add_argument("--tournament-ids", action="append", required=True)

    prepare = commands.add_parser("prepare-review")
    prepare.add_argument("--tournament-ids", action="append", required=True)
    prepare.add_argument("--output", required=True)
    prepare.add_argument("--competitions", default=str(COMPETITIONS_PATH))

    for name in ("approve-batch", "reject-batch"):
        batch = commands.add_parser(name)
        batch.add_argument("--input", required=True)
    return parser


def _read(path: Path) -> Mapping[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        document = json.load(handle)
    SofaScoreCatalog.from_mapping(document)
    return document


def _reviewed_at(value: Any) -> str:
    if value is None:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return _required_text(value, "reviewed_at")


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ActivationError(f"{field} is required")
    return value.strip()


def _evidence_items(values: Sequence[Any]) -> list[dict[str, Any]]:
    """Accept CLI reference strings and pre-rendered evidence objects alike."""

    items: list[dict[str, Any]] = []
    for value in values:
        if isinstance(value, str):
            if value.strip():
                items.append(
                    {"type": "operator_reference", "reference": value.strip()}
                )
        elif isinstance(value, Mapping):
            items.append(deepcopy(dict(value)))
        else:
            raise ActivationError(f"unsupported review evidence item: {value!r}")
    return items


def _tournament_ids(values: Sequence[str]) -> list[int]:
    ids: list[int] = []
    for value in values:
        for token in str(value).split(","):
            token = token.strip()
            if not token:
                continue
            try:
                tournament_id = int(token)
            except ValueError as exc:
                raise ValueError(f"invalid tournament id: {token!r}") from exc
            if tournament_id in ids:
                raise ValueError(f"duplicate tournament id: {tournament_id}")
            ids.append(tournament_id)
    if not ids:
        raise ValueError("at least one tournament id is required")
    return ids


def _positions(document: Mapping[str, Any]) -> dict[int, int]:
    positions: dict[int, int] = {}
    for index, item in enumerate(document["tournaments"]):
        tournament_id = int(item["unique_tournament_id"])
        if tournament_id in positions:
            raise ValueError(
                f"registry must contain tournament {tournament_id} exactly once"
            )
        positions[tournament_id] = index
    return positions


def _position(positions: Mapping[int, int], tournament_id: int) -> int:
    try:
        return positions[tournament_id]
    except KeyError as exc:
        raise ValueError(
            f"registry must contain tournament {tournament_id} exactly once"
        ) from exc


def _competition_seasons(path: str | Path) -> dict[str, set[str]]:
    document = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    seasons: dict[str, set[str]] = {}
    for competition in document.get("competitions") or []:
        seasons[str(competition["id"])] = {
            str(season["id"]).strip()
            for season in (competition.get("seasons") or [])
            if season.get("id") is not None
        }
    return seasons


def _review_draft(
    tournament: Mapping[str, Any],
    competitions: Mapping[str, set[str]],
) -> dict[str, Any]:
    """Render one editable approval row plus the evidence an operator must add."""

    classification = tournament.get("classification") or {}
    canonical_id = tournament.get("canonical_id")
    canonical_seasons = sorted(
        {
            str(season["canonical_season"])
            for season in (tournament.get("seasons") or [])
            if isinstance(season, Mapping) and season.get("canonical_season")
        }
    )

    blocked: list[str] = []
    evidence: list[dict[str, Any]] = []
    if not canonical_id:
        blocked.append("canonical_id is missing from the registry row")
    elif canonical_id not in competitions:
        blocked.append(
            f"canonical_id {canonical_id!r} is not in "
            "configs/medallion/competitions.yaml"
        )
    else:
        evidence.append(
            {
                "type": "repository",
                "reference": f"configs/medallion/competitions.yaml#{canonical_id}",
                "note": REVIEW_TODO,
                # Sentinel: approval fails while this stub is unreplaced, so
                # filling in only reviewed_by cannot activate a competition.
                REVIEW_EVIDENCE_TODO_KEY: True,
            }
        )
        if not canonical_seasons:
            blocked.append(
                "registry has no canonical source season; run targeted discovery"
            )
        elif not set(canonical_seasons) & competitions[canonical_id]:
            blocked.append(
                f"no canonical season of {canonical_id!r} overlaps "
                "configs/medallion/competitions.yaml"
            )

    return {
        "tournament_id": int(tournament["unique_tournament_id"]),
        "canonical_id": canonical_id,
        "evidence": evidence,
        "notes": None,
        "source_snapshot": {
            "name": tournament.get("name"),
            "gender": classification.get("gender"),
            "age_group": classification.get("age_group"),
            "team_level": classification.get("team_level"),
            "status": classification.get("status"),
            "exclusion_reasons": list(classification.get("exclusion_reasons") or []),
            "canonical_seasons": canonical_seasons,
        },
        "blocked": blocked,
    }


def _apply_batch(
    updated: dict[str, Any],
    command: str,
    payload: Mapping[str, Any],
    positions: Mapping[int, int],
) -> list[int]:
    """Apply every reviewed row in memory; any failure aborts the whole wave."""

    key = "approvals" if command == "approve-batch" else "rejections"
    rows = payload.get(key)
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"batch file must contain a non-empty {key!r} list")
    reviewed_by = _required_text(payload.get("reviewed_by"), "reviewed_by")
    reviewed_at = _reviewed_at(payload.get("reviewed_at"))

    applied: list[int] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError(f"batch {key} rows must be objects")
        tournament_id = int(row["tournament_id"])
        if tournament_id in applied:
            raise ValueError(f"duplicate tournament id: {tournament_id}")
        index = _position(positions, tournament_id)
        tournament = updated["tournaments"][index]
        evidence = _evidence_items(row.get("evidence") or [])
        if command == "approve-batch":
            replacement = approve_tournament(
                tournament,
                canonical_id=_required_text(row.get("canonical_id"), "canonical_id"),
                reviewed_by=reviewed_by,
                reviewed_at=reviewed_at,
                evidence=evidence,
                notes=row.get("notes"),
            )
        else:
            replacement = reject_tournament(
                tournament,
                reviewed_by=reviewed_by,
                reviewed_at=reviewed_at,
                evidence=evidence,
                notes=row.get("notes"),
            )
        updated["tournaments"][index] = replacement
        applied.append(tournament_id)
    return applied


def _write_review_file(path: Path, approvals: Sequence[Mapping[str, Any]]) -> None:
    document = {
        "generated_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
        "reviewed_by": None,
        "reviewed_at": None,
        "approvals": list(approvals),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    path = Path(args.registry).resolve()
    current = _read(path)
    command = args.command

    if command == "prepare-review":
        tournament_ids = _tournament_ids(args.tournament_ids)
        positions = _positions(current)
        competitions = _competition_seasons(args.competitions)
        approvals = [
            _review_draft(
                current["tournaments"][_position(positions, tournament_id)],
                competitions,
            )
            for tournament_id in tournament_ids
        ]
        output = Path(args.output)
        _write_review_file(output, approvals)
        print(json.dumps({
            "status": "success",
            "command": command,
            "tournament_ids": tournament_ids,
            "output": str(output),
            "blocked": [
                row["tournament_id"] for row in approvals if row["blocked"]
            ],
        }, sort_keys=True))
        return 0

    updated = deepcopy(dict(current))
    positions = _positions(updated)
    if command in {"approve-batch", "reject-batch"}:
        with Path(args.input).open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        tournament_ids = _apply_batch(updated, command, payload, positions)
    elif command in {"enable", "disable"}:
        tournament_ids = _tournament_ids(args.tournament_ids)
        for tournament_id in tournament_ids:
            index = _position(positions, tournament_id)
            updated["tournaments"][index] = set_activation(
                updated["tournaments"][index], enabled=command == "enable"
            )
    else:
        tournament_ids = [args.tournament_id]
        index = _position(positions, args.tournament_id)
        tournament = updated["tournaments"][index]
        evidence = _evidence_items(args.evidence)
        if command == "approve":
            updated["tournaments"][index] = approve_tournament(
                tournament,
                canonical_id=args.canonical_id,
                reviewed_by=args.reviewed_by,
                reviewed_at=_reviewed_at(args.reviewed_at),
                evidence=evidence,
                notes=args.notes,
            )
        else:
            updated["tournaments"][index] = reject_tournament(
                tournament,
                reviewed_by=args.reviewed_by,
                reviewed_at=_reviewed_at(args.reviewed_at),
                evidence=evidence,
                notes=args.notes,
            )

    SofaScoreCatalog.from_mapping(updated)
    changed = write_registry_atomic(path, updated, expected_current=current)
    print(json.dumps({
        "status": "success",
        "command": command,
        "tournament_ids": tournament_ids,
        "changed": changed,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
