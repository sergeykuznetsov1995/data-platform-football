"""Source-native domain objects for FotMob discovery and parsing.

The objects in this module deliberately do not use the medallion competition
names or integer season convention.  FotMob's numeric competition id and the
exact season string returned by the source are the only identity fields.
Human-readable names and slugs are presentation metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
import unicodedata
from typing import Any, Mapping, Optional, Tuple


JsonRow = dict[str, Any]
Rows = Tuple[JsonRow, ...]


class ScopeDecision(str, Enum):
    """Decision made by the discovery scope policy."""

    INCLUDED = "included"
    EXCLUDED = "excluded"
    REVIEW_REQUIRED = "review_required"


@dataclass(frozen=True, slots=True)
class CompetitionRef:
    """A FotMob competition discovered from ``allLeagues``.

    ``name`` and ``presentation_slug`` must never be used as storage keys.  A
    competition can be renamed or translated while ``competition_id`` stays
    stable.
    """

    competition_id: int
    name: str
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    gender: Optional[str] = None
    competition_type: Optional[str] = None
    age_group: Optional[str] = None
    page_url: Optional[str] = None
    source_slug: Optional[str] = None
    source_paths: Tuple[str, ...] = ()

    @property
    def identity(self) -> int:
        return self.competition_id

    @property
    def presentation_slug(self) -> str:
        return competition_slug(self.competition_id, self.source_slug or self.name)


@dataclass(frozen=True, slots=True)
class SeasonRef:
    """An exact FotMob season key belonging to a competition."""

    competition_id: int
    source_season_key: str
    is_selected: bool = False
    is_latest: bool = False
    source_order: Optional[int] = None

    def __post_init__(self) -> None:
        if not isinstance(self.source_season_key, str) or not self.source_season_key:
            raise ValueError("source_season_key must be a non-empty source string")

    @property
    def identity(self) -> tuple[int, str]:
        return (self.competition_id, self.source_season_key)


@dataclass(frozen=True, slots=True)
class StageRef:
    """A stage/table/round inside an exact competition season."""

    competition_id: int
    source_season_key: str
    stage_id: str
    name: Optional[str] = None
    stage_type: Optional[str] = None
    parent_stage_id: Optional[str] = None
    source_path: Optional[str] = None
    source_order: Optional[int] = None
    page_url: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.source_season_key:
            raise ValueError("source_season_key must not be empty")
        if not self.stage_id:
            raise ValueError("stage_id must not be empty")

    @property
    def identity(self) -> tuple[int, str, str]:
        return (self.competition_id, self.source_season_key, self.stage_id)


@dataclass(frozen=True, slots=True)
class ScopeRef:
    """The immutable source scope used by fetch, parse, and manifest layers."""

    competition_id: int
    source_season_key: str
    stage_id: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.source_season_key, str) or not self.source_season_key:
            raise ValueError("source_season_key must be a non-empty source string")

    @classmethod
    def from_season(cls, season: SeasonRef, stage_id: Optional[str] = None) -> "ScopeRef":
        return cls(season.competition_id, season.source_season_key, stage_id)

    @property
    def identity(self) -> tuple[int, str, Optional[str]]:
        return (self.competition_id, self.source_season_key, self.stage_id)


@dataclass(frozen=True, slots=True)
class ScopeClassification:
    """Auditable inclusion decision for a discovered competition."""

    competition: CompetitionRef
    decision: ScopeDecision
    reason: str
    policy_rule: str


@dataclass(frozen=True, slots=True)
class LeaderboardCategoryRef:
    """A leaderboard advertised by a league payload.

    The descriptor is kept even when its ``fetch_all_url`` is absent or a
    later request fails, which lets the manifest distinguish unavailable data
    from a category that was silently skipped.
    """

    participant_type: str
    name: Optional[str]
    header: Optional[str]
    category: Optional[str]
    fetch_all_url: Optional[str]
    localized_title_id: Optional[str] = None
    source_order: Optional[int] = None
    preview_count: int = 0


@dataclass(frozen=True, slots=True)
class ParseIssue:
    """A non-fatal shape or identity issue found while parsing."""

    code: str
    path: str
    message: str


@dataclass(frozen=True, slots=True)
class SeasonBundle:
    """One-pass normalized view of a ``/leagues`` season payload."""

    scope: ScopeRef
    details: Mapping[str, Any]
    capabilities: Mapping[str, Any]
    matches: Rows = ()
    standings: Rows = ()
    stages: Rows = ()
    playoffs: Rows = ()
    teams: Rows = ()
    player_categories: Tuple[LeaderboardCategoryRef, ...] = ()
    team_categories: Tuple[LeaderboardCategoryRef, ...] = ()
    json_paths: Tuple[str, ...] = ()
    issues: Tuple[ParseIssue, ...] = ()


def competition_slug(competition_id: int, value: str) -> str:
    """Build a stable, id-prefixed presentation slug.

    The numeric prefix makes URLs and report labels unambiguous.  It is still
    presentation-only: callers must use ``competition_id`` for joins/keys.
    """

    text = unicodedata.normalize("NFKD", str(value or ""))
    ascii_text = text.encode("ascii", "ignore").decode("ascii").lower()
    words = re.sub(r"[^a-z0-9]+", "-", ascii_text).strip("-")
    return f"{int(competition_id)}-{words}" if words else str(int(competition_id))


__all__ = [
    "CompetitionRef",
    "JsonRow",
    "LeaderboardCategoryRef",
    "ParseIssue",
    "Rows",
    "ScopeClassification",
    "ScopeDecision",
    "ScopeRef",
    "SeasonBundle",
    "SeasonRef",
    "StageRef",
    "competition_slug",
]
