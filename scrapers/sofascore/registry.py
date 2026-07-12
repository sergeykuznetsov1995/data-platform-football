"""Schema-v2 helpers for the SofaScore source registry.

The registry deliberately separates evidence obtained from SofaScore from an
operator decision.  A positive-looking competition name is never sufficient
to enable capture: the upstream payload must explicitly identify men's
football and an operator must confirm adult, first-team participation.
"""

from __future__ import annotations

import re
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence


SCHEMA_VERSION = 2
SUPPORTED_SCHEMA_VERSIONS = frozenset({1, SCHEMA_VERSION})

_WOMEN_RE = re.compile(
    r"(?:^|\W)(?:women(?:'s)?|woman|ladies|female|feminin(?:e)?|"
    r"femenin[ao]|femminile|frauen)(?:\W|$)",
    re.IGNORECASE,
)
_YOUTH_RE = re.compile(
    r"(?:^|\W)(?:u[- ]?\d{1,2}|under[- ]?\d{1,2}|youth|junior(?:s)?|"
    r"juvenil|primavera)(?:\W|$)",
    re.IGNORECASE,
)
_RESERVE_RE = re.compile(
    r"(?:^|\W)(?:reserve(?:s)?|reserva(?:s)?|second[- ]team|b[- ]team|"
    r"jong)(?:\W|$)",
    re.IGNORECASE,
)
_FUTSAL_RE = re.compile(r"(?:^|\W)futsal(?:\W|$)", re.IGNORECASE)

_MALE = frozenset({"m", "male", "men", "man"})
_FEMALE = frozenset({"f", "female", "women", "woman"})
_MIXED = frozenset({"mixed", "mix", "coed", "co-ed", "x"})
_ADULT = frozenset({"adult", "adults", "senior", "seniors"})
_FIRST_TEAM = frozenset({"first", "first_team", "first-team", "senior"})


class ActivationError(ValueError):
    """Capture activation was requested without sufficient evidence."""


@dataclass(frozen=True)
class ActivationEligibility:
    """A stable, inspectable production-capture decision."""

    allowed: bool
    reasons: tuple[str, ...]

    def require(self) -> None:
        if not self.allowed:
            raise ActivationError("; ".join(self.reasons))


def _source_value(raw: Mapping[str, Any], *keys: str) -> tuple[Optional[str], Any]:
    for key in keys:
        if key in raw:
            return key, raw.get(key)
    return None, None


def normalize_gender(value: Any) -> str:
    """Map source gender tokens to a closed vocabulary without guessing."""

    if value is None:
        return "unknown"
    token = str(value).strip().casefold()
    if token in _MALE:
        return "male"
    if token in _FEMALE:
        return "female"
    if token in _MIXED:
        return "mixed"
    return "unknown"


def _source_true(value: Any) -> bool:
    if value is True or value == 1:
        return True
    return isinstance(value, str) and value.strip().casefold() in {
        "true", "yes", "1",
    }


def _evidence(field: str, value: Any, endpoint: str) -> dict[str, Any]:
    return {
        "type": "source_field",
        "endpoint": endpoint,
        "field": field,
        "value": value,
    }


def classify_tournament_source(
    raw: Mapping[str, Any],
    *,
    name: str,
    sport_slug: str,
    endpoint: str,
) -> dict[str, Any]:
    """Classify a tournament using only source evidence.

    Unknown values stay unknown.  In particular, the lack of ``Women`` or
    ``U21`` in a name is not converted into positive adult-men evidence.
    Explicit negative evidence always wins over an operator review later.
    """

    evidence: list[dict[str, Any]] = [
        _evidence("category.sport.slug", sport_slug, endpoint),
        _evidence("name", name, endpoint),
    ]
    exclusions: list[str] = []

    gender_key, raw_gender = _source_value(raw, "gender", "sex")
    gender = normalize_gender(raw_gender)
    if gender_key is not None:
        evidence.append(_evidence(gender_key, raw_gender, endpoint))
    if gender == "female":
        exclusions.append("female competition")
    elif gender == "mixed":
        exclusions.append("mixed-gender competition")

    age_group = "unknown"
    age_key, raw_age = _source_value(raw, "ageGroup", "ageCategory", "age_group")
    if age_key is not None:
        evidence.append(_evidence(age_key, raw_age, endpoint))
        age_token = str(raw_age).strip().casefold()
        if age_token in _ADULT:
            age_group = "adult"
        elif _YOUTH_RE.search(age_token):
            age_group = "youth"
            exclusions.append("youth competition")
    youth_key, raw_is_youth = _source_value(raw, "isYouth", "youth")
    if youth_key is not None:
        evidence.append(_evidence(youth_key, raw_is_youth, endpoint))
        if _source_true(raw_is_youth):
            age_group = "youth"
            exclusions.append("youth competition")

    team_level = "unknown"
    level_key, raw_level = _source_value(raw, "teamLevel", "team_level")
    if level_key is not None:
        evidence.append(_evidence(level_key, raw_level, endpoint))
        level_token = str(raw_level).strip().casefold().replace(" ", "_")
        if level_token in _FIRST_TEAM:
            team_level = "first_team"
        elif "reserve" in level_token or level_token in {"b", "ii"}:
            team_level = "reserve"
            exclusions.append("reserve competition")
    reserve_key, raw_is_reserve = _source_value(raw, "isReserve", "reserve")
    if reserve_key is not None:
        evidence.append(_evidence(reserve_key, raw_is_reserve, endpoint))
        if _source_true(raw_is_reserve):
            team_level = "reserve"
            exclusions.append("reserve competition")

    if sport_slug != "football":
        exclusions.append(f"sport is {sport_slug or 'unknown'}, not football")
    if _WOMEN_RE.search(name):
        exclusions.append("women marker in source name")
    if _YOUTH_RE.search(name):
        age_group = "youth"
        exclusions.append("youth marker in source name")
    if _RESERVE_RE.search(name):
        team_level = "reserve"
        exclusions.append("reserve marker in source name")
    if _FUTSAL_RE.search(name) or sport_slug == "futsal":
        exclusions.append("futsal competition")

    exclusions = sorted(set(exclusions))
    if exclusions:
        status = "excluded"
    elif gender != "male" or sport_slug != "football":
        status = "unknown"
    elif age_group == "adult" and team_level == "first_team":
        status = "source_confirmed_adult_men"
    else:
        status = "review_required"

    evidence.sort(key=lambda item: (item["field"], repr(item["value"])))
    return {
        "sport": sport_slug or "unknown",
        "gender": gender,
        "age_group": age_group,
        "team_level": team_level,
        "status": status,
        "exclusion_reasons": exclusions,
        "evidence": evidence,
    }


def pending_review() -> dict[str, Any]:
    """Return the canonical operator-owned default for a new tournament."""

    return {
        "status": "pending",
        "confirmed": {
            "sport": None,
            "gender": None,
            "age_group": None,
            "team_level": None,
        },
        "reviewed_by": None,
        "reviewed_at": None,
        "evidence": [],
        "notes": None,
    }


def _meaningful_review_evidence(value: Any) -> bool:
    if not isinstance(value, list) or not value:
        return False
    for item in value:
        if not isinstance(item, Mapping):
            continue
        if any(
            isinstance(item.get(field), str) and item[field].strip()
            for field in ("reference", "url", "note", "value")
        ):
            return True
    return False


def activation_eligibility(tournament: Mapping[str, Any]) -> ActivationEligibility:
    """Decide whether a registry row may enter any production capture path."""

    reasons: list[str] = []
    if not tournament.get("canonical_id"):
        reasons.append("canonical_id is missing")

    classification = tournament.get("classification")
    if not isinstance(classification, Mapping):
        reasons.append("source classification evidence is missing")
        classification = {}
    if classification.get("sport") != "football":
        reasons.append("source sport is not confirmed football")
    if classification.get("gender") != "male":
        reasons.append("source gender is not confirmed male")
    # An operator may resolve source fields that are genuinely unknown, but
    # explicit negative source evidence is never overridable.  Keep these
    # checks independent from ``exclusion_reasons`` so a stale/hand-edited
    # classification cannot turn a youth or reserve competition into an adult
    # first-team capture merely by clearing the explanatory list.
    if classification.get("age_group") == "youth":
        reasons.append("source age_group is youth")
    if classification.get("team_level") == "reserve":
        reasons.append("source team_level is reserve")
    exclusions = classification.get("exclusion_reasons")
    if isinstance(exclusions, list) and exclusions:
        reasons.extend(f"source exclusion: {value}" for value in exclusions)
    if classification.get("status") in {None, "unknown", "excluded"}:
        reasons.append("source classification is not an adult-men candidate")

    review = tournament.get("review")
    if not isinstance(review, Mapping):
        reasons.append("operator review is missing")
        review = {}
    if review.get("status") != "approved":
        reasons.append("operator review is not approved")
    confirmed = review.get("confirmed")
    if not isinstance(confirmed, Mapping):
        confirmed = {}
    expected = {
        "sport": "football",
        "gender": "male",
        "age_group": "adult",
        "team_level": "first_team",
    }
    for field, value in expected.items():
        if confirmed.get(field) != value:
            reasons.append(f"operator review does not confirm {field}={value}")
    review_evidence = review.get("evidence")
    if not _meaningful_review_evidence(review_evidence):
        reasons.append("operator review evidence is missing")

    seasons = tournament.get("seasons")
    if not isinstance(seasons, list) or not any(
        isinstance(season, Mapping) and season.get("canonical_season")
        for season in seasons
    ):
        reasons.append("no canonical source season is available")

    return ActivationEligibility(not reasons, tuple(dict.fromkeys(reasons)))


def ensure_capture_allowed(tournament: Mapping[str, Any]) -> None:
    """Raise a human-readable error unless capture is fail-closed eligible."""

    activation_eligibility(tournament).require()


def approve_tournament(
    tournament: Mapping[str, Any],
    *,
    canonical_id: str,
    reviewed_by: str,
    reviewed_at: str,
    evidence: Sequence[Mapping[str, Any]],
    notes: Optional[str] = None,
) -> dict[str, Any]:
    """Return an approved but still-disabled operator projection."""

    if not str(canonical_id).strip():
        raise ActivationError("canonical_id is required")
    if not str(reviewed_by).strip() or not str(reviewed_at).strip():
        raise ActivationError("reviewed_by and reviewed_at are required")
    if not _meaningful_review_evidence(list(evidence)):
        raise ActivationError("at least one review evidence item is required")
    updated = deepcopy(dict(tournament))
    updated["canonical_id"] = str(canonical_id).strip()
    updated["enabled"] = False
    updated["review"] = {
        "status": "approved",
        "confirmed": {
            "sport": "football",
            "gender": "male",
            "age_group": "adult",
            "team_level": "first_team",
        },
        "reviewed_by": str(reviewed_by).strip(),
        "reviewed_at": str(reviewed_at).strip(),
        "evidence": [deepcopy(dict(item)) for item in evidence],
        "notes": notes,
    }
    # Source exclusions and unknown gender cannot be overridden by a reviewer.
    eligibility = activation_eligibility(updated)
    source_reasons = tuple(
        reason for reason in eligibility.reasons
        if reason.startswith("source ")
    )
    if source_reasons:
        raise ActivationError("; ".join(source_reasons))
    return updated


def set_activation(tournament: Mapping[str, Any], *, enabled: bool) -> dict[str, Any]:
    """Toggle the operator flag, enforcing eligibility on activation."""

    updated = deepcopy(dict(tournament))
    if enabled:
        activation_eligibility(updated).require()
    updated["enabled"] = bool(enabled)
    return updated


def reject_tournament(
    tournament: Mapping[str, Any],
    *,
    reviewed_by: str,
    reviewed_at: str,
    evidence: Sequence[Mapping[str, Any]],
    notes: Optional[str] = None,
) -> dict[str, Any]:
    """Record an evidenced rejection and force the tournament disabled."""

    if not str(reviewed_by).strip() or not str(reviewed_at).strip():
        raise ActivationError("reviewed_by and reviewed_at are required")
    if not _meaningful_review_evidence(list(evidence)):
        raise ActivationError("at least one review evidence item is required")
    updated = deepcopy(dict(tournament))
    updated["enabled"] = False
    updated["review"] = {
        "status": "rejected",
        "confirmed": {
            "sport": None,
            "gender": None,
            "age_group": None,
            "team_level": None,
        },
        "reviewed_by": str(reviewed_by).strip(),
        "reviewed_at": str(reviewed_at).strip(),
        "evidence": [deepcopy(dict(item)) for item in evidence],
        "notes": notes,
    }
    return updated


__all__ = [
    "ActivationEligibility",
    "ActivationError",
    "SCHEMA_VERSION",
    "SUPPORTED_SCHEMA_VERSIONS",
    "activation_eligibility",
    "approve_tournament",
    "classify_tournament_source",
    "ensure_capture_allowed",
    "normalize_gender",
    "pending_review",
    "reject_tournament",
    "set_activation",
]
