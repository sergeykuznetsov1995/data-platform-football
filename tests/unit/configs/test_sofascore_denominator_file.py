"""Contract of configs/sofascore/denominator.tsv (#1353)."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from pathlib import Path

import pytest

from scrapers.sofascore.denominator import (
    CLASS_PRIORITY,
    COLUMNS,
    DEFAULT_DENOMINATOR_PATH,
    DenominatorError,
    load_denominator,
)

ROOT = Path(__file__).resolve().parents[3]
CONFIG = ROOT / "configs" / "sofascore"
ESOCCER = re.compile(r"esoccer|fifa ?\d\d|eliga|efootball|\besports?\b", re.I)
STUDENT = re.compile(r"NCAA|U-Sports|CCAA|Universit|College|Student", re.I)


def _raw_rows():
    with DEFAULT_DENOMINATOR_PATH.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t", quoting=csv.QUOTE_NONE))


def _registry_only():
    registry = json.loads((CONFIG / "tournaments.json").read_text(encoding="utf-8"))
    return {
        int(row["unique_tournament_id"]): row["canonical_id"]
        for row in registry["tournaments"]
        if row["unique_tournament_id"] in (8, 16, 17, 23, 34, 35, 203)
    }


@pytest.mark.unit
def test_file_parses_with_unique_ids_and_known_classes():
    denominator = load_denominator(DEFAULT_DENOMINATOR_PATH)
    rows = _raw_rows()

    assert tuple(rows[0].keys()) == COLUMNS
    assert len(denominator.rows) == len(rows)
    assert {row.tournament_class for row in denominator.rows.values()} <= set(
        CLASS_PRIORITY
    )


@pytest.mark.unit
def test_class_decides_queue_priority():
    assert CLASS_PRIORITY == {
        "core": 1,
        "esoccer": 0, "student": 0,
        "youth": 9, "reserve": 9, "amateur": 9, "show": 9, "women": 9,
        "unknown": 9,
    }
    for row in load_denominator(DEFAULT_DENOMINATOR_PATH).rows.values():
        assert row.queue_priority == CLASS_PRIORITY[row.tournament_class]


@pytest.mark.unit
def test_seven_daily_registry_tournaments_are_core_under_their_canonical_id():
    denominator = load_denominator(DEFAULT_DENOMINATOR_PATH)
    registry = _registry_only()

    assert len(registry) == 7
    for tournament_id, canonical_id in registry.items():
        row = denominator.rows[tournament_id]
        assert row.tournament_class == "core"
        assert row.capture_key == canonical_id


@pytest.mark.unit
def test_esoccer_is_outside_the_queues_and_students_are_student():
    rows = load_denominator(DEFAULT_DENOMINATOR_PATH).rows.values()

    esoccer = [row for row in rows if ESOCCER.search(row.name)]
    students = [row for row in rows if STUDENT.search(row.name)]
    assert esoccer and students
    assert all(row.queue_priority == 0 for row in esoccer)
    assert all(row.tournament_class == "student" for row in students)


@pytest.mark.unit
def test_core_is_at_least_1300_tournaments():
    denominator = load_denominator(DEFAULT_DENOMINATOR_PATH)

    assert len(denominator.core_capture_keys()) >= 1300


@pytest.mark.unit
def test_every_campaign_candidate_is_in_the_file():
    """The file covers exactly the 1 504 ids the campaign policy locks."""

    policy = json.loads(
        (CONFIG / "all_mens_campaign.json").read_text(encoding="utf-8")
    )
    registry = set(_registry_only())
    ids = sorted(
        tournament_id
        for tournament_id in load_denominator(DEFAULT_DENOMINATOR_PATH).rows
        if tournament_id not in registry
    )

    # Same formula as ``candidate_ids_digest`` in all_mens_campaign.py.
    assert len(ids) == policy["candidate_count"]
    assert hashlib.sha256(
        json.dumps(ids, separators=(",", ":")).encode()
    ).hexdigest() == policy["candidate_ids_sha256"]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("body", "message"),
    [
        ("1\tSS-1\tx\tcore\t9\tb\n", "must have queue_priority 1"),
        ("1\tSS-1\tx\tpro\t1\tb\n", "unknown class"),
        ("1\tSS-1\tx\tcore\t1\tb\n1\tSS-1\tx\tcore\t1\tb\n", "duplicate"),
        ("1\tSS-1\tx\tcore\t1\n", "expected 6 columns"),
        ("x\tSS-1\tx\tcore\t1\tb\n", "non-integer"),
        ("", "no rows"),
    ],
)
def test_malformed_file_fails_closed(tmp_path, body, message):
    path = tmp_path / "denominator.tsv"
    path.write_text("\t".join(COLUMNS) + "\n" + body, encoding="utf-8")

    with pytest.raises(DenominatorError, match=message):
        load_denominator(path)


@pytest.mark.unit
def test_env_path_wins_and_a_missing_file_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("SOFASCORE_DENOMINATOR_PATH", str(tmp_path / "absent.tsv"))

    with pytest.raises(DenominatorError, match="cannot read"):
        load_denominator()
