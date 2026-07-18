"""Domain identities shared by the WhoScored ingestion pipeline.

The old scraper accepted a mixture of season start years and four digit
season slugs.  Values such as ``2021`` are inherently ambiguous: they can mean
the 2020/21 club season or the 2021 edition of a calendar-year tournament.
This module deliberately does not guess.  A scope always carries the season
format that was resolved by the competition catalog.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
from typing import Optional


_SEASON_ID_RE = re.compile(
    r"^(?P<base>[0-9]{4})"
    r"(?:-(?P<qualifier>single|split|multi)-ws(?P<source_id>[1-9][0-9]*))?$"
)
_SEASON_FORMAT_QUALIFIER = {
    "single_year": "single",
    "split_year": "split",
    "multi_year": "multi",
}


class SeasonFormat(str, Enum):
    """How a canonical four-digit season id must be interpreted."""

    SPLIT_YEAR = "split_year"
    SINGLE_YEAR = "single_year"
    # Some international editions span a non-consecutive range after a
    # postponement (for example ``2019/2021``).  Preserve that source identity
    # explicitly instead of misclassifying or dropping the season.
    MULTI_YEAR = "multi_year"

    @classmethod
    def coerce(cls, value: "SeasonFormat | str") -> "SeasonFormat":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip())
        except ValueError as exc:
            allowed = ", ".join(item.value for item in cls)
            raise ValueError(
                f"Unsupported season format {value!r}; expected one of: {allowed}"
            ) from exc


class TournamentEligibility(str, Enum):
    """Explicit catalog disposition for every tournament seen at the source.

    ``QUARANTINED`` is deliberately a first-class state.  Discovery may fan out
    through quarantined tournaments to obtain authoritative ``sex`` metadata,
    but a production ingestion scope must never treat one as eligible.
    """

    INCLUDED = "included"
    EXCLUDED_WOMEN = "excluded_women"
    EXCLUDED_YOUTH = "excluded_youth"
    EXCLUDED_RESERVE = "excluded_reserve"
    EXCLUDED_TECHNICAL = "excluded_technical"
    SOURCE_UNAVAILABLE = "source_unavailable"
    QUARANTINED = "quarantined"

    @classmethod
    def coerce(cls, value: "TournamentEligibility | str") -> "TournamentEligibility":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip())
        except ValueError as exc:
            allowed = ", ".join(item.value for item in cls)
            raise ValueError(
                f"Unsupported tournament eligibility {value!r}; expected: {allowed}"
            ) from exc


@dataclass(frozen=True)
class TournamentClassification:
    """Auditable inclusion decision, including the classifier version."""

    eligibility: TournamentEligibility
    reason: str
    classifier_version: str
    source_sex: Optional[int] = None
    override_version: Optional[str] = None

    @property
    def is_eligible(self) -> bool:
        return self.eligibility is TournamentEligibility.INCLUDED


def canonical_season_id(
    value: str | int,
    season_format: SeasonFormat | str,
) -> str:
    """Validate and return a canonical season id without converting it.

    ``2526`` and ``2026`` are already canonical source-independent ids.  This
    function never accepts a start year and never shifts a value by one year.
    The explicit format is what makes the otherwise ambiguous ``2021`` safe.
    """

    token = str(value).strip()
    fmt = SeasonFormat.coerce(season_format)
    match = _SEASON_ID_RE.fullmatch(token)
    if match is None:
        raise ValueError(
            "Season id must be four decimal digits or a strict disambiguated "
            f"source identity, got {value!r}"
        )
    base = match.group("base")
    qualifier = match.group("qualifier")
    expected_qualifier = _SEASON_FORMAT_QUALIFIER[fmt.value]
    if qualifier is not None and qualifier != expected_qualifier:
        raise ValueError(
            f"Season id {token!r} encodes {qualifier!r}, expected "
            f"{expected_qualifier!r} for {fmt.value}"
        )

    if fmt in {SeasonFormat.SPLIT_YEAR, SeasonFormat.MULTI_YEAR}:
        first = int(base[:2])
        second = int(base[2:])
        consecutive = second == (first + 1) % 100
        if fmt is SeasonFormat.SPLIT_YEAR and not consecutive:
            raise ValueError(
                f"Split-year season {token!r} must contain consecutive YY values"
            )
        if fmt is SeasonFormat.MULTI_YEAR and consecutive:
            raise ValueError(
                f"Multi-year season {token!r} must not be an adjacent split year"
            )
    else:
        year = int(base)
        if not 1800 <= year <= 2199:
            raise ValueError(
                f"Single-year season {token!r} must be a four-digit calendar year"
            )
    return token


def base_season_id(value: str | int) -> str:
    """Return the four-digit base of a canonical or disambiguated identity."""

    token = str(value).strip()
    match = _SEASON_ID_RE.fullmatch(token)
    if match is None:
        raise ValueError(f"Invalid canonical season identity {value!r}")
    return match.group("base")


def source_season_id_hint(value: str | int) -> Optional[int]:
    """Return the immutable WhoScored source id encoded by a collision key."""

    token = str(value).strip()
    match = _SEASON_ID_RE.fullmatch(token)
    if match is None:
        raise ValueError(f"Invalid canonical season identity {value!r}")
    source_id = match.group("source_id")
    return int(source_id) if source_id is not None else None


def disambiguated_season_id(
    value: str | int,
    season_format: SeasonFormat | str,
    source_season_id: str | int,
) -> str:
    """Build a stable lossless identity for a real canonical collision.

    The ordinary cross-source id remains four digits.  A suffix is introduced
    only when one competition exposes multiple distinct source seasons with
    that same base; the immutable source id prevents a later third edition
    from renaming the already discovered pair.
    """

    fmt = SeasonFormat.coerce(season_format)
    base = base_season_id(canonical_season_id(value, fmt))
    source_token = str(source_season_id).strip()
    if re.fullmatch(r"[1-9][0-9]*", source_token) is None:
        raise ValueError(
            f"source_season_id must be a positive integer, got {source_season_id!r}"
        )
    token = f"{base}-{_SEASON_FORMAT_QUALIFIER[fmt.value]}-ws{source_token}"
    return canonical_season_id(token, fmt)


@dataclass(frozen=True, order=True)
class WhoScoredScope:
    """One competition-season pair, the only legal unit of source work."""

    competition_id: str
    season_id: str
    season_format: SeasonFormat

    def __post_init__(self) -> None:
        competition_id = str(self.competition_id).strip()
        if not competition_id:
            raise ValueError("competition_id must not be empty")
        if "=" in competition_id:
            raise ValueError("competition_id must not contain '='")
        fmt = SeasonFormat.coerce(self.season_format)
        season_id = canonical_season_id(self.season_id, fmt)
        object.__setattr__(self, "competition_id", competition_id)
        object.__setattr__(self, "season_id", season_id)
        object.__setattr__(self, "season_format", fmt)

    @property
    def spec(self) -> str:
        """Stable CLI/report representation (for example ``INT-World Cup=2026``)."""

        return f"{self.competition_id}={self.season_id}"

    @classmethod
    def parse(
        cls,
        spec: str,
        *,
        season_format: SeasonFormat | str,
    ) -> "WhoScoredScope":
        """Parse a scope spec after its format has been resolved by a catalog."""

        if not isinstance(spec, str) or spec.count("=") != 1:
            raise ValueError(
                "WhoScored scope must have the form '<competition>=<season-id>'"
            )
        competition_id, season_id = (part.strip() for part in spec.split("=", 1))
        return cls(competition_id, season_id, SeasonFormat.coerce(season_format))
