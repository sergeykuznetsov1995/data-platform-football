"""Contract of configs/espn/denominator.tsv (#1499)."""

from __future__ import annotations

import csv

import pytest

from scrapers.espn.classify import CLASSES
from scrapers.espn.denominator import (
    COLUMNS,
    DEFAULT_DENOMINATOR_PATH,
    QUEUE_PRIORITY,
    DenominatorError,
    load_denominator,
)

# The 20 leagues of the old 181-league native list that are not adult men's
# official competitions (review 24.09, C7).
EXCLUDED = {
    "usa.ncaa.m.1": "college",
    "bangabandhu.cup": "friendly",
    "club.friendly": "friendly",
    "esp.joan_gamper": "friendly",
    "fifa.friendly": "friendly",
    "fifa.intercontinental.cup": "friendly",
    "friendly.emirates_cup": "friendly",
    "jpn.world_challenge": "friendly",
    "nonfifa": "friendly",
    "fifa.concacaf.olympicsq": "olympic",
    "fifa.conmebol.olympicsq": "olympic",
    "fifa.olympics": "olympic",
    "concacaf.u23": "youth",
    "fifa.friendly_u21": "youth",
    "fifa.world.u17": "youth",
    "fifa.world.u20": "youth",
    "global.u20.intercontinental_cup": "youth",
    "uefa.euro.u19": "youth",
    "uefa.euro_u21": "youth",
    "uefa.euro_u21_qual": "youth",
}
ADDED = ("afc.champions_qual", "concacaf.champions_cup")


def _raw_rows():
    with DEFAULT_DENOMINATOR_PATH.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t", quoting=csv.QUOTE_NONE))


def _row(**overrides):
    row = dict.fromkeys(COLUMNS, "")
    row.update(
        slug="eng.1", espn_id="700", name="English Premier League",
        espn_gender="MALE", **{"class": "senior_official"},
        class_reason="rule:default", in_target="1", hidden="0", live="1",
    )
    row.update(overrides)
    return "\t".join(row[column] for column in COLUMNS) + "\n"


@pytest.mark.unit
def test_file_parses_with_unique_slugs_and_known_classes():
    denominator = load_denominator(DEFAULT_DENOMINATOR_PATH)
    rows = _raw_rows()

    assert tuple(rows[0].keys()) == COLUMNS
    assert len(rows) == 223
    assert len(denominator.rows) == len(rows)
    assert [row["slug"] for row in rows] == sorted(row["slug"] for row in rows)
    assert {row.tournament_class for row in denominator.rows.values()} <= set(
        CLASSES
    )


@pytest.mark.unit
def test_class_vocabulary_and_queue_priority():
    assert set(QUEUE_PRIORITY) == set(CLASSES)
    assert QUEUE_PRIORITY == {
        "senior_official": 1,
        "youth": 9, "olympic": 9, "friendly": 9, "reserve": 9,
        "women": 0, "college": 0,
    }
    denominator = load_denominator(DEFAULT_DENOMINATOR_PATH)
    assert denominator.queue_priority("eng.1") == 1
    assert denominator.queue_priority("fifa.world.u20") == 9
    assert denominator.queue_priority("usa.ncaa.m.1") == 0
    assert denominator.queue_priority("no.such.league") is None


@pytest.mark.unit
def test_target_is_exactly_visible_senior_official():
    denominator = load_denominator(DEFAULT_DENOMINATOR_PATH)

    for slug, row in denominator.rows.items():
        assert row.in_target == (
            row.tournament_class == "senior_official" and not row.hidden
        ), slug
    assert len(denominator.targets()) == 163
    assert denominator.live_targets() <= denominator.targets()
    assert len(denominator.live_targets()) == 161


@pytest.mark.unit
def test_twenty_excluded_are_out_and_two_added_are_in():
    denominator = load_denominator(DEFAULT_DENOMINATOR_PATH)

    assert len(EXCLUDED) == 20
    for slug, klass in EXCLUDED.items():
        assert not denominator.is_target(slug), slug
        assert denominator.row(slug).tournament_class == klass, slug
    for slug in ADDED:
        assert denominator.is_target(slug), slug
    assert denominator.row("concacaf.champions_cup").class_reason.startswith(
        "manual:"
    )


@pytest.mark.unit
def test_hidden_swiss_league_is_senior_but_outside_the_percentage():
    row = load_denominator(DEFAULT_DENOMINATOR_PATH).row("sui.1")

    assert row.tournament_class == "senior_official"
    assert row.hidden and not row.in_target and not row.live


@pytest.mark.unit
def test_cross_source_ids_and_deep_level_are_empty_until_filled():
    for row in load_denominator(DEFAULT_DENOMINATOR_PATH).rows.values():
        assert (row.fotmob_id, row.sofascore_id, row.deep_level) == ("", "", "")


@pytest.mark.unit
@pytest.mark.parametrize(
    ("body", "message"),
    [
        (_row(**{"class": "pro"}, in_target="0"), "unknown class"),
        (_row() + _row(), "duplicate slug"),
        (_row(**{"class": "women"}, espn_gender="FEMALE"), "in_target must be 1"),
        (_row(hidden="1"), "in_target must be 1"),
        (_row(in_target="0"), "in_target must be 1"),
        (_row(live="yes"), "live must be 0 or 1"),
        (_row(class_reason="because"), "class_reason"),
        (_row(espn_id="x"), "non-integer espn_id"),
        ("eng.1\t700\n", "expected 14 columns"),
        ("", "no rows"),
    ],
)
def test_malformed_file_fails_closed(tmp_path, body, message):
    path = tmp_path / "denominator.tsv"
    path.write_text("\t".join(COLUMNS) + "\n" + body, encoding="utf-8")

    with pytest.raises(DenominatorError, match=message):
        load_denominator(path)


@pytest.mark.unit
def test_unknown_header_fails_closed(tmp_path):
    path = tmp_path / "denominator.tsv"
    path.write_text("slug\tclass\n" + "eng.1\tsenior_official\n", encoding="utf-8")

    with pytest.raises(DenominatorError, match="unexpected header"):
        load_denominator(path)


@pytest.mark.unit
def test_env_path_wins_and_a_missing_file_fails_closed(tmp_path, monkeypatch):
    good = tmp_path / "good.tsv"
    good.write_text("\t".join(COLUMNS) + "\n" + _row(), encoding="utf-8")
    monkeypatch.setenv("ESPN_DENOMINATOR_PATH", str(good))
    assert load_denominator().targets() == frozenset({"eng.1"})

    monkeypatch.setenv("ESPN_DENOMINATOR_PATH", str(tmp_path / "absent.tsv"))
    with pytest.raises(DenominatorError, match="cannot read"):
        load_denominator()
