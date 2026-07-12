"""Versioned static contract for WhoScored's Detailed statistics options.

The team and player Detailed tabs currently advertise the same 25
``category``/``subcategory`` pairs.  They are intentionally modelled as a
closed, reviewed fetch catalog.  The current bootstrap response contains HTML
that only references external JavaScript bundles; it does not expose their
bodies.  Consequently this module deliberately makes no claim of runtime
bundle discovery.  A source-contract change requires an explicit catalog
version and pinned semantic-fingerprint update.

The inspected bundle evidence did not establish a stable, source-authored
``sortBy`` value for every Detailed option.  Consequently every entry uses the
endpoint's explicit empty fallback.  A future source-derived sort must carry
its evidence and requires a catalog version bump.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
from types import MappingProxyType
from typing import Iterable, Mapping


DETAILED_FEED_CATALOG_VERSION = "2026-07-11.1"
TEAM_DETAILED_STATISTICS_ENDPOINT = (
    "https://www.whoscored.com/statisticsfeed/1/getteamstatistics"
)
PLAYER_DETAILED_STATISTICS_ENDPOINT = (
    "https://www.whoscored.com/statisticsfeed/1/getplayerstatistics"
)


class DetailedFeedFamily(str, Enum):
    """WhoScored statistics endpoint families that expose Detailed options."""

    TEAM = "team"
    PLAYER = "player"


class SortByProvenance(str, Enum):
    """Why a catalog entry carries its ``sort_by`` value."""

    SOURCE_BUNDLE = "source_bundle"
    SAFE_EMPTY_FALLBACK = "safe_empty_fallback"


@dataclass(frozen=True, order=True, slots=True)
class DetailedFeedOption:
    """One exact source-authored Detailed category/subcategory pair."""

    category: str
    subcategory: str

    def __post_init__(self) -> None:
        for field_name, value in (
            ("category", self.category),
            ("subcategory", self.subcategory),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"{field_name} must be a non-empty string")
            if value != value.strip() or value != value.lower():
                raise ValueError(
                    f"{field_name} must preserve the lowercase source token, got "
                    f"{value!r}"
                )
            if not ("a" <= value[0] <= "z") or any(
                not (
                    "a" <= character <= "z"
                    or "0" <= character <= "9"
                    or character == "-"
                )
                for character in value
            ):
                raise ValueError(f"invalid {field_name} source token {value!r}")


@dataclass(frozen=True, order=True, slots=True)
class DetailedFeedSpec:
    """Immutable fetch specification for one option on one endpoint family."""

    catalog_version: str
    family: DetailedFeedFamily
    endpoint: str
    option: DetailedFeedOption
    sort_by: str
    sort_by_provenance: SortByProvenance
    sort_by_evidence: str | None = None

    def __post_init__(self) -> None:
        expected_endpoint = _ENDPOINT_BY_FAMILY[self.family]
        if self.catalog_version != DETAILED_FEED_CATALOG_VERSION:
            raise ValueError(
                f"entry version {self.catalog_version!r} does not match catalog "
                f"version {DETAILED_FEED_CATALOG_VERSION!r}"
            )
        if self.endpoint != expected_endpoint:
            raise ValueError(
                f"{self.family.value} entry must use {expected_endpoint!r}, got "
                f"{self.endpoint!r}"
            )
        if self.sort_by_provenance is SortByProvenance.SOURCE_BUNDLE:
            if not self.sort_by or not self.sort_by_evidence:
                raise ValueError(
                    "source-bundle sort_by requires a value and evidence reference"
                )
        elif self.sort_by or self.sort_by_evidence is not None:
            raise ValueError(
                "safe empty sort fallback must have sort_by='' and no evidence"
            )

    @property
    def category(self) -> str:
        return self.option.category

    @property
    def subcategory(self) -> str:
        return self.option.subcategory

    @property
    def key(self) -> str:
        """Stable work-item identity suitable for manifests and task mapping."""

        return f"{self.family.value}:{self.category}:{self.subcategory}"


_ENDPOINT_BY_FAMILY: Mapping[DetailedFeedFamily, str] = {
    DetailedFeedFamily.TEAM: TEAM_DETAILED_STATISTICS_ENDPOINT,
    DetailedFeedFamily.PLAYER: PLAYER_DETAILED_STATISTICS_ENDPOINT,
}


DETAILED_FEED_OPTIONS: tuple[DetailedFeedOption, ...] = tuple(
    DetailedFeedOption(category, subcategory)
    for category, subcategory in (
        ("shots", "zones"),
        ("shots", "situations"),
        ("shots", "accuracy"),
        ("shots", "bodyparts"),
        ("goals", "zones"),
        ("goals", "situations"),
        ("goals", "bodyparts"),
        ("conversion", "zones"),
        ("conversion", "situations"),
        ("passes", "length"),
        ("passes", "type"),
        ("key-passes", "length"),
        ("key-passes", "type"),
        ("assists", "type"),
        ("blocks", "type"),
        ("offsides", "type"),
        ("fouls", "type"),
        ("cards", "type"),
        ("possession-loss", "type"),
        ("dribbles", "success"),
        ("tackles", "success"),
        ("interception", "success"),
        ("clearances", "success"),
        ("aerial", "success"),
        ("saves", "shotzone"),
    )
)


DETAILED_FEED_CATALOG: tuple[DetailedFeedSpec, ...] = tuple(
    DetailedFeedSpec(
        catalog_version=DETAILED_FEED_CATALOG_VERSION,
        family=family,
        endpoint=_ENDPOINT_BY_FAMILY[family],
        option=option,
        sort_by="",
        sort_by_provenance=SortByProvenance.SAFE_EMPTY_FALLBACK,
    )
    for family in DetailedFeedFamily
    for option in DETAILED_FEED_OPTIONS
)


def fingerprint_detailed_feed_catalog(
    catalog: Iterable[DetailedFeedSpec] = DETAILED_FEED_CATALOG,
) -> str:
    """Return a deterministic SHA-256 over the semantic catalog contract."""

    rows = sorted(
        (
            {
                "catalog_version": entry.catalog_version,
                "family": entry.family.value,
                "endpoint": entry.endpoint,
                "category": entry.category,
                "subcategory": entry.subcategory,
                "sort_by": entry.sort_by,
                "sort_by_provenance": entry.sort_by_provenance.value,
                "sort_by_evidence": entry.sort_by_evidence,
            }
            for entry in catalog
        ),
        key=lambda row: (row["family"], row["category"], row["subcategory"]),
    )
    payload = json.dumps(
        rows,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


# This value is deliberately pinned instead of being derived at import time.  A
# derived value only describes whatever code happens to be deployed; it cannot
# detect an accidental catalog edit without a version bump.  Registering a new
# version and its reviewed digest is therefore an explicit source-contract
# change.
_PINNED_CATALOG_FINGERPRINTS: Mapping[str, str] = MappingProxyType(
    {
        "2026-07-11.1": (
            "a58764cb4c3baf5c781690f34b791fccd653cb3279bff14b71793c5e36e72c2b"
        ),
    }
)
DETAILED_FEED_CATALOG_FINGERPRINT = _PINNED_CATALOG_FINGERPRINTS[
    DETAILED_FEED_CATALOG_VERSION
]


def validate_detailed_feed_catalog(
    catalog: Iterable[DetailedFeedSpec] = DETAILED_FEED_CATALOG,
) -> None:
    """Fail import/startup if the static catalog changed without a version bump."""

    entries = tuple(catalog)
    options = frozenset(DETAILED_FEED_OPTIONS)
    if len(DETAILED_FEED_OPTIONS) != 25 or len(options) != 25:
        raise RuntimeError("WhoScored Detailed option catalog must contain 25 pairs")
    if len(entries) != 50:
        raise RuntimeError("WhoScored Detailed feed catalog must contain 50 entries")
    keys = {entry.key for entry in entries}
    if len(keys) != len(entries):
        raise RuntimeError("WhoScored Detailed feed catalog contains duplicate keys")
    for family in DetailedFeedFamily:
        family_options = {entry.option for entry in entries if entry.family is family}
        if family_options != options:
            raise RuntimeError(
                f"WhoScored Detailed {family.value} catalog is incomplete"
            )
    actual_fingerprint = fingerprint_detailed_feed_catalog(entries)
    if actual_fingerprint != DETAILED_FEED_CATALOG_FINGERPRINT:
        raise RuntimeError(
            "WhoScored Detailed catalog fingerprint changed without a reviewed "
            f"version bump: version={DETAILED_FEED_CATALOG_VERSION!r} "
            f"expected={DETAILED_FEED_CATALOG_FINGERPRINT} "
            f"actual={actual_fingerprint}"
        )


validate_detailed_feed_catalog()


__all__ = [
    "DETAILED_FEED_CATALOG",
    "DETAILED_FEED_CATALOG_FINGERPRINT",
    "DETAILED_FEED_CATALOG_VERSION",
    "DETAILED_FEED_OPTIONS",
    "PLAYER_DETAILED_STATISTICS_ENDPOINT",
    "TEAM_DETAILED_STATISTICS_ENDPOINT",
    "DetailedFeedFamily",
    "DetailedFeedOption",
    "DetailedFeedSpec",
    "SortByProvenance",
    "fingerprint_detailed_feed_catalog",
    "validate_detailed_feed_catalog",
]
