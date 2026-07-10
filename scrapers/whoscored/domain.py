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


_SEASON_ID_RE = re.compile(r"^[0-9]{4}$")


class SeasonFormat(str, Enum):
    """How a canonical four-digit season id must be interpreted."""

    SPLIT_YEAR = "split_year"
    SINGLE_YEAR = "single_year"

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
    if not _SEASON_ID_RE.fullmatch(token):
        raise ValueError(
            f"Season id must be exactly four decimal digits, got {value!r}"
        )

    if fmt is SeasonFormat.SPLIT_YEAR:
        first = int(token[:2])
        second = int(token[2:])
        if second != (first + 1) % 100:
            raise ValueError(
                f"Split-year season {token!r} must contain consecutive YY values"
            )
    else:
        year = int(token)
        if not 1800 <= year <= 2199:
            raise ValueError(
                f"Single-year season {token!r} must be a four-digit calendar year"
            )
    return token


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
