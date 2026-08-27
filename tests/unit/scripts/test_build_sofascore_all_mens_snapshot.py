from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scripts.build_sofascore_all_mens_snapshot import (
    SnapshotError,
    build_snapshot,
    snapshot_digest,
)


POLICY = {
    "schema_version": 1,
    "scope": "adult_men_professional_leagues_and_cups",
    "candidate_count": 2,
    "allowed_kinds": ["cup", "league"],
}


def _candidate_doc() -> dict:
    return {
        "count": 2,
        "candidates": [
            {
                "id": 200,
                "name": "Example Cup",
                "category": "World",
                "slug": "example-cup",
            },
            {
                "id": 100,
                "name": "Example League",
                "category": "Exampleland",
                "slug": "example-league",
            },
        ],
    }


def _recount_doc() -> dict:
    def row(source_id: int, name: str, category: str, slug: str, seasons: list[dict]):
        return {
            "unique_tournament_id": source_id,
            "name": name,
            "slug": slug,
            "category": {
                "id": source_id + 1,
                "name": category,
                "slug": category.casefold().replace(" ", "-"),
            },
            "sport_slug": "football",
            "page_path": (
                f"football/{category.casefold().replace(' ', '-')}/{slug}"
            ),
            "canonical_id": None,
            "enabled": False,
            "classification": {
                "sport": "football",
                "gender": "unknown",
                "age_group": "unknown",
                "team_level": "unknown",
                "status": "unknown",
                "exclusion_reasons": [],
                "evidence": [],
            },
            "review": {"status": "pending"},
            "seasons": seasons,
        }

    return {
        "schema_version": 2,
        "tournaments": [
            row(
                100,
                "Example League",
                "Exampleland",
                "example-league",
                [
                    {
                        "season_id": 1002,
                        "source_name": "2025/26",
                        "canonical_season": "2526",
                        "season_format": "split_year",
                    },
                    {
                        "season_id": 1001,
                        "source_name": "2024/25",
                        "canonical_season": "2425",
                        "season_format": "split_year",
                    },
                ],
            ),
            row(
                200,
                "Example Cup",
                "World",
                "example-cup",
                [
                    {
                        "season_id": 2001,
                        "source_name": "Example Cup 2025",
                        "canonical_season": "2025",
                        "season_format": "single_year",
                    }
                ],
            ),
        ],
    }


def _estimates() -> list[dict]:
    return [
        {"id": 200, "name": "Example Cup", "seasons": 1, "type": "cup"},
        {"id": 100, "name": "Example League", "seasons": 2, "type": "league"},
    ]


def test_snapshot_is_exact_deterministic_owner_scope():
    first = build_snapshot(_candidate_doc(), _recount_doc(), _estimates(), POLICY)
    second = build_snapshot(
        copy.deepcopy(_candidate_doc()),
        copy.deepcopy(_recount_doc()),
        copy.deepcopy(_estimates()),
        copy.deepcopy(POLICY),
    )

    assert first == second
    assert first["candidate_count"] == 2
    assert len(first["campaign_id"]) == 64
    assert first["snapshot_id"] == snapshot_digest(first)
    assert [row["unique_tournament_id"] for row in first["tournaments"]] == [100, 200]
    assert [
        season["canonical_season"] for season in first["tournaments"][0]["seasons"]
    ] == ["2526", "2425"]
    assert all(
        season["metadata_status"] == "pending"
        for row in first["tournaments"]
        for season in row["seasons"]
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("gender", "female"),
        ("gender", "mixed"),
        ("age_group", "youth"),
        ("team_level", "reserve"),
    ],
)
def test_known_non_adult_mens_candidate_is_frozen_as_excluded(field: str, value: str):
    recount = _recount_doc()
    recount["tournaments"][0]["classification"][field] = value
    recount["tournaments"][0]["classification"]["status"] = "excluded"

    snapshot = build_snapshot(_candidate_doc(), recount, _estimates(), POLICY)
    row = snapshot["tournaments"][0]
    assert row["metadata_status"] == "excluded"
    assert row["classification"]["status"] == "excluded"
    assert all(s["metadata_status"] == "excluded" for s in row["seasons"])


def test_amateur_candidate_is_excluded_even_when_classification_is_unknown():
    candidates = _candidate_doc()
    recount = _recount_doc()
    candidates["candidates"][0]["category"] = "Exampleland Amateur"
    recount["tournaments"][0]["category"]["name"] = "Exampleland Amateur"

    snapshot = build_snapshot(candidates, recount, _estimates(), POLICY)
    row = snapshot["tournaments"][0]
    assert row["metadata_status"] == "excluded"
    assert "amateur marker in source identity" in row["classification"][
        "exclusion_reasons"
    ]


@pytest.mark.parametrize(
    ("canonical", "expected"),
    [("9900", 1999), ("0001", 2000), ("2526", 2025)],
)
def test_split_season_century_is_resolved(canonical: str, expected: int):
    recount = _recount_doc()
    season = recount["tournaments"][0]["seasons"][0]
    season["canonical_season"] = canonical
    season["source_name"] = canonical

    snapshot = build_snapshot(_candidate_doc(), recount, _estimates(), POLICY)

    actual = next(
        s for s in snapshot["tournaments"][0]["seasons"]
        if s["source_season_id"] == 1002
    )
    assert actual["start_year"] == expected


def test_missing_duplicate_and_extra_rows_fail_closed():
    recount = _recount_doc()
    recount["tournaments"].pop()
    with pytest.raises(SnapshotError, match="missing from recount"):
        build_snapshot(_candidate_doc(), recount, _estimates(), POLICY)

    duplicated = _candidate_doc()
    duplicated["candidates"][1]["id"] = 200
    with pytest.raises(SnapshotError, match="duplicate candidate id 200"):
        build_snapshot(duplicated, _recount_doc(), _estimates(), POLICY)

    estimates = _estimates() + [
        {"id": 999, "name": "Not approved", "seasons": 1, "type": "cup"}
    ]
    with pytest.raises(SnapshotError, match="estimate ids differ"):
        build_snapshot(_candidate_doc(), _recount_doc(), estimates, POLICY)


def test_snapshot_round_trips_as_canonical_utf8_json(tmp_path: Path):
    snapshot = build_snapshot(_candidate_doc(), _recount_doc(), _estimates(), POLICY)
    path = tmp_path / "snapshot.json"
    path.write_text(
        json.dumps(snapshot, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )

    assert json.loads(path.read_text(encoding="utf-8")) == snapshot
