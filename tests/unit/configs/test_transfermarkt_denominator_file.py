"""Contract of configs/transfermarkt/denominator.tsv (#1390)."""

from __future__ import annotations

import csv

import pytest

from scrapers.transfermarkt.denominator import (
    COLUMNS,
    DEFAULT_DENOMINATOR_PATH,
    DenominatorError,
    denominator_ids,
    load_denominator,
)

# The milestone-1 denominator the file fixes: live core competitions of the
# canonical registry snapshot tm-discovery-71d704b010cdbd222b4fe27c.
EXPECTED_DENOMINATOR = 547
EXPECTED_POKAL = 205
NATIONAL_TEAM_TOURNAMENTS = {
    "FIWC", "EURO", "COPA", "AFCN", "AFAC", "GOCU", "WMQ1", "WMQ6", "UNLA",
    "UNLD", "CNLA", "FS", "EMQ", "AFCQ",
}


def _raw_rows():
    with DEFAULT_DENOMINATOR_PATH.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t", quoting=csv.QUOTE_NONE))


def test_file_loads_and_fixes_the_denominator():
    denominator = load_denominator()
    ids = denominator.denominator_ids()
    assert len(ids) == EXPECTED_DENOMINATOR
    assert ids == denominator_ids()
    assert sum(denominator.rows[cid].route == "pokal" for cid in ids) == EXPECTED_POKAL


def test_every_row_has_country_tier_class_route_live_and_a_named_name():
    rows = _raw_rows()
    assert len(rows) == len({row["id"] for row in rows})
    for row in rows:
        for field in ("country", "tier", "class", "route", "live", "reason"):
            assert row[field].strip(), (row["id"], field)
        assert any(char.isalpha() for char in row["name"]), row["id"]
        assert "%" not in row["name"], row["id"]


def test_national_team_tournaments_are_core_national():
    rows = {row["id"]: row for row in _raw_rows()}
    for cid in NATIONAL_TEAM_TOURNAMENTS:
        assert rows[cid]["class"] == "core_national", cid
    # A club competition is never filed as a national one.
    for cid in ("GB1", "CL", "BRC", "FAC"):
        if cid in rows:
            assert rows[cid]["class"] == "core_club", cid


def test_archive_is_exactly_not_live_and_amateur_is_outside_the_denominator():
    denominator = load_denominator()
    for row in denominator.rows.values():
        assert (row.competition_class == "archive") == (not row.live)
        if row.competition_class in {"amateur", "youth", "reserve", "archive"}:
            assert not row.is_core


def test_current_saison_id_follows_the_one_season_rule():
    rows = {row["id"]: row for row in _raw_rows()}
    # Calendar edition: saison_id is the year before the printed year.
    assert rows["BRC"]["current_saison_id"] == "2025"
    assert rows["BRA1"]["current_saison_id"] == "2025"
    assert rows["GB1"]["current_saison_id"] == "2026"
    assert rows["CC92"]["current_saison_id"] == "1991"


def test_loader_fails_closed_on_malformed_rows(tmp_path):
    good = [
        "GB1", "Premier League", "England", "UEFA", "1", "core_club",
        "wettbewerb", "1", "2026", "", "", "", "", "rule:default",
    ]
    header = "\t".join(COLUMNS)

    def write(*rows):
        path = tmp_path / "denominator.tsv"
        path.write_text(
            "\n".join([header, *("\t".join(row) for row in rows)]) + "\n",
            encoding="utf-8",
        )
        return path

    assert load_denominator(write(good)).denominator_ids() == {"GB1"}
    bad_cases = [
        [*good[:1], "38.1 %", *good[2:]],
        [*good[:5], "senior", *good[6:]],
        [*good[:2], "", *good[3:]],
        [*good[:7], "0", *good[8:]],
        [*good[:8], "26", *good[9:]],
    ]
    for bad in bad_cases:
        with pytest.raises(DenominatorError):
            load_denominator(write(bad))
    with pytest.raises(DenominatorError, match="duplicate"):
        load_denominator(write(good, good))
