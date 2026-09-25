"""Tournament class of an ESPN league from its slug, name and gender (#1499).

ESPN has no age or level field, so the class is a rule over ``slug``/``name``
plus two small manual tables.  The order is fixed: a manual men override, then
women, college, olympic, youth, friendly (by name/slug, then the manual
invitational table), reserve, and finally ``senior_official``.  Only
``senior_official`` counts toward the freshness percentage and the history
queue; ``configs/espn/denominator.tsv`` stores the result per slug.

Pure functions only: no paths, no I/O.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

CLASSES = (
    "senior_official",
    "youth",
    "olympic",
    "friendly",
    "college",
    "reserve",
    "women",
)

MEN_OVERRIDE = {
    "concacaf.champions_cup": (
        "ESPN core gender=FEMALE ошибочно: это мужской Кубок чемпионов "
        "КОНКАКАФ 1962-2008 (женский - concacaf.w.champions_cup)"
    ),
}
FRIENDLY_SLUGS = {"fifa.friendly", "club.friendly", "nonfifa"}
INVITATIONAL = {  # предсезонные/пригласительные турниры = товарищеские по сути
    "esp.joan_gamper": "Trofeo Joan Gamper - предсезонный матч Барселоны",
    "jpn.world_challenge": (
        "J.League World Challenge - предсезонные матчи с европейскими клубами"
    ),
    "fifa.intercontinental.cup": (
        "Intercontinental Cup (India) - пригласительный турнир сборных AIFF "
        "(спорно: матчи категории A)"
    ),
    "bangabandhu.cup": "Bangabandhu Cup - пригласительный турнир сборных (Бангладеш)",
}
YOUTH_RE = re.compile(
    r"(?:^|[._])u-?(1[5-9]|2[0-3])(?:$|[._])|_u(1[5-9]|2[0-3])"
    r"|under-?\s?(1[5-9]|2[0-3])|\bu-?(1[5-9]|2[0-3])\b",
    re.I,
)
WOMEN_RE = re.compile(
    r"(^|\.)w\.|\.w$|wchampions|wwc|wworld|weuro|\.womens|femenina"
    r"|copa_de_la_reina|nwsl|shebelieves|pinatar|arnold\.clark|women",
    re.I,
)
_WOMEN_NAME_RE = re.compile(r"\bwomen", re.I)
_RESERVE_RE = re.compile(r"reserve|\bII\b|\.b$|_b$")


@dataclass(frozen=True)
class Classification:
    cls: str
    reason: str  # ``rule:<name>`` or ``manual:<text>``


def classify(slug: str, name: str, gender: str) -> Classification:
    """Class of one ESPN league; ``gender`` is ESPN's raw value (``MALE``…)."""

    if slug in MEN_OVERRIDE:
        return Classification("senior_official", "manual:" + MEN_OVERRIDE[slug])
    if gender == "FEMALE":
        return Classification("women", "rule:women_gender")
    if WOMEN_RE.search(slug):
        return Classification("women", "rule:women_slug")
    if _WOMEN_NAME_RE.search(name):
        return Classification("women", "rule:women_name")
    if slug.startswith("usa.ncaa"):
        return Classification("college", "rule:college")
    if "olympic" in slug or "olympic" in name.lower():
        return Classification("olympic", "rule:olympic")
    if YOUTH_RE.search(slug) or YOUTH_RE.search(name):
        return Classification("youth", "rule:youth")
    if (
        slug in FRIENDLY_SLUGS
        or slug.startswith("friendly.")
        or "friendly" in name.lower()
    ):
        return Classification("friendly", "rule:friendly")
    if slug in INVITATIONAL:
        return Classification("friendly", "manual:" + INVITATIONAL[slug])
    if _RESERVE_RE.search(slug + " " + name):
        return Classification("reserve", "rule:reserve")
    return Classification("senior_official", "rule:default")


__all__ = [
    "CLASSES",
    "Classification",
    "FRIENDLY_SLUGS",
    "INVITATIONAL",
    "MEN_OVERRIDE",
    "WOMEN_RE",
    "YOUTH_RE",
    "classify",
]
