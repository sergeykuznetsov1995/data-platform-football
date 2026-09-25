"""The ESPN class rule on the 24.09 catalog matches denominator.tsv (#1499)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scrapers.espn.classify import CLASSES, classify
from scrapers.espn.denominator import DEFAULT_DENOMINATOR_PATH, load_denominator

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "espn"
DETAILS = ("afc.champions_qual", "fifa.world.u20", "concacaf.champions_cup", "sui.1")


def _inputs() -> dict[str, tuple[str, str]]:
    """slug -> (name, gender) from the catalog and the recorded details."""

    catalog = json.loads(
        (FIXTURES / "catalog_2026-07-31.json").read_text(encoding="utf-8")
    )
    inputs = {c["slug"]: (c["name"], c["gender"]) for c in catalog["candidates"]}
    for slug in DETAILS:
        detail = json.loads(
            (FIXTURES / "probes" / f"league_detail_{slug}.json").read_text(
                encoding="utf-8"
            )
        )
        assert detail["slug"] == slug
        inputs[slug] = (detail["name"], detail["gender"])
    return inputs


@pytest.mark.unit
def test_rule_on_the_catalog_reproduces_the_file():
    denominator = load_denominator(DEFAULT_DENOMINATOR_PATH)
    inputs = _inputs()

    assert len(inputs) == 222
    # The one file row without a recorded body: guessed from its slug alone.
    assert set(denominator.rows) - set(inputs) == {"mex.w.1"}
    assert set(inputs) <= set(denominator.rows)
    for slug, row in denominator.rows.items():
        name, gender = inputs.get(slug, (row.name, row.espn_gender))
        result = classify(slug, name, gender)
        assert result.cls == row.tournament_class, slug
        assert result.reason == row.class_reason, slug
        assert row.in_target == (result.cls == "senior_official" and not row.hidden)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("slug", "name", "gender", "cls", "reason"),
    [
        ("concacaf.champions_cup", "CONCACAF Champions Cup", "FEMALE",
         "senior_official", "manual:"),
        ("fifa.world.u20", "FIFA Under-20 World Cup", "MALE", "youth", "rule:youth"),
        ("usa.ncaa.m.1", "NCAA Men's Soccer", "MALE", "college", "rule:college"),
        ("esp.joan_gamper", "Trofeo Joan Gamper", "MALE", "friendly", "manual:"),
        ("eng.trophy", "English EFL Trophy", "MALE", "senior_official",
         "rule:default"),
        ("usa.open", "U.S. Open Cup", "MALE", "senior_official", "rule:default"),
        ("fifa.olympics", "Men's Olympic Soccer Tournament", "MALE", "olympic",
         "rule:olympic"),
        ("fifa.wwcq.ply", "FIFA Women's World Cup Qualifying", "UNKNOWN",
         "women", "rule:women_slug"),
        ("xyz.1", "Some Women Cup", "MALE", "women", "rule:women_name"),
        ("esp.reserves", "Spanish Reserves", "MALE", "reserve", "rule:reserve"),
        ("ger.3", "Bayern II", "MALE", "reserve", "rule:reserve"),
        ("xyz.b", "Some League", "MALE", "reserve", "rule:reserve"),
        ("xyz_b", "Some League", "MALE", "reserve", "rule:reserve"),
    ],
)
def test_point_cases(slug, name, gender, cls, reason):
    result = classify(slug, name, gender)

    assert result.cls == cls
    assert result.reason.startswith(reason)
    assert result.cls in CLASSES
