"""Versioned current denominator derived from a promoted Transfermarkt registry."""

from __future__ import annotations

import os
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from dags.utils.transfermarkt_scope_planner import (
    _competition_from_joined_row,
    _edition_from_joined_row,
)
from scrapers.transfermarkt.denominator import (
    Denominator,
    DenominatorError,
    DenominatorRow,
)
from scrapers.transfermarkt.registry import (
    AgeCategory,
    CompetitionType,
    EvidenceOrigin,
    Gender,
    TeamType,
    season_window_year,
)

TIER_BY_SECTION = {
    "First Tier": "1",
    "Second Tier": "2",
    "Third Tier": "3",
    "Fourth Tier": "4",
    "Fifth Tier": "5",
    "Sixth Tier": "6",
    "Play-Offs": "playoff",
    "Regional Championship": "regional",
    "Domestic Cup": "cup",
    "Further Cup": "cup",
    "League Cup": "cup",
    "Domestic Super Cup": "supercup",
    "Further Super Cup": "supercup",
    "Cups": "international",
    "International cups": "international",
    "International cup competitions": "international",
    "National Team Competitions": "international",
}
ARCHIVE_SECTION = "National leagues and cups currently not being played"
YOUTH_SPECIAL_IDS = frozenset({"AG18", "CACG"})
AMATEUR_SPECIAL_IDS = frozenset({"NPAM"})
CONTINENT_GEOGRAPHY = frozenset(
    {"Africa", "Americas", "Asia", "Europe", "World", "International", "Unknown", ""}
)
# Archived profile geography missing from the continental catalogue row.
# The edition's source_body_hash identifies the exact profile with this metadata.
PROFILE_COUNTRIES = {
    (
        "BRRD",
        "https://www.transfermarkt.com/campeonato-rondoniense/startseite/pokalwettbewerb/BRRD",
        "2e1ef6ee432b81fe84130c0b255a73f4355f2cca5afbea9e1788f64fcc3d2d19",
    ): ("Americas", "Brazil"),
}


@dataclass(frozen=True)
class UniverseEntry:
    row: DenominatorRow
    registry_status: str
    team_type: str
    section_labels: tuple[str, ...]
    title_only: bool
    live_decision: bool | None
    class_subtype: str | None = None
    country_evidence_sha256: str | None = None


@dataclass(frozen=True)
class RegistryDenominator:
    denominator: Denominator
    universe_version: str
    entries: tuple[UniverseEntry, ...]
    quarantined: tuple[tuple[str, str], ...]
    title_only_ids: tuple[str, ...] = ()


def denominator_source(configured: str | None = None) -> str:
    value = (
        (configured or os.environ.get("TRANSFERMARKT_DENOMINATOR_SOURCE", "tsv"))
        .strip()
        .lower()
    )
    if value not in {"tsv", "registry"}:
        raise DenominatorError(f"unknown denominator source: {value!r}")
    return value


def current_denominator_snapshot_id(
    requested: str | None = None, *, source: str | None = None
) -> str | None:
    """Resolve the exact snapshot for replay or a deployment-pinned current read."""
    if requested is None or (isinstance(requested, str) and requested == ""):
        if denominator_source(source) != "registry":
            return None
        requested = os.environ.get("TRANSFERMARKT_DENOMINATOR_REGISTRY_SNAPSHOT_ID", "")
    if not isinstance(requested, str) or requested != requested.strip():
        raise DenominatorError("registry snapshot ID must be an exact string")
    return requested or None


def _duplicate_identity(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in row.items()
        if key not in {"last_success_at", "career_fetches_pending"}
    }


def registry_snapshot_id(rows: Iterable[Mapping[str, Any]]) -> str:
    if not rows:
        raise DenominatorError("registry denominator is empty")
    snapshots = set()
    for row in rows:
        if not isinstance(row, Mapping):
            raise DenominatorError("registry row must be a mapping")
        version = row.get("registry_snapshot_id")
        if not isinstance(version, str) or not version or version != version.strip():
            raise DenominatorError("registry denominator requires one exact snapshot")
        snapshots.add(version)
    if len(snapshots) != 1:
        raise DenominatorError("registry denominator requires one exact snapshot")
    return snapshots.pop()


def _canonical_rows(
    registry_rows: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, Mapping[str, Any]], str]:
    rows = tuple(registry_rows)
    universe_version = registry_snapshot_id(rows)
    canonical: dict[str, Mapping[str, Any]] = {}
    identities: dict[str, dict[str, Any]] = {}
    for row in rows:
        competition_id = str(row.get("competition_id") or "").strip()
        if not competition_id:
            raise DenominatorError("registry row has no competition_id")
        if row.get("is_current") is not True:
            raise DenominatorError(f"{competition_id}: registry row is not current")
        identity = _duplicate_identity(row)
        if competition_id in identities and identities[competition_id] != identity:
            raise DenominatorError(
                f"{competition_id}: conflicting current registry rows"
            )
        identities[competition_id] = identity
        canonical[competition_id] = row
    return canonical, universe_version


def _quarantine_rows(
    quarantined: Mapping[str, str] | None,
) -> tuple[tuple[str, str], ...]:
    if quarantined is None:
        return ()
    if not isinstance(quarantined, Mapping):
        raise DenominatorError("quarantine must be an id-to-reason mapping")
    result = []
    for competition_id, reason in quarantined.items():
        cid = str(competition_id).strip()
        text = str(reason).strip()
        if not cid or not text:
            raise DenominatorError("quarantine IDs and reasons must be nonempty")
        result.append((cid, text))
    return tuple(sorted(result))


def _title_ids(title_only_ids: Iterable[str]) -> tuple[str, ...]:
    values = {str(competition_id).strip() for competition_id in title_only_ids}
    if "" in values:
        raise DenominatorError("title-only competition ID is empty")
    return tuple(sorted(values))


def _tier(record) -> tuple[str, tuple[str, ...]]:
    evidence = [
        item
        for item in record.evidence
        if item.source_field == "section_label"
        and item.competition_type == record.competition_type
        and item.source_value != ARCHIVE_SECTION
    ]
    if not evidence:
        if record.competition_type in {
            CompetitionType.CONTINENTAL_CLUB,
            CompetitionType.NATIONAL_TEAM_TOURNAMENT,
        }:
            return "international", ()
        if record.competition_type is CompetitionType.DOMESTIC_CUP:
            return "cup", ()
        return "unknown", ()
    narrowest = max(item.precedence for item in evidence)
    labels = tuple(
        sorted({item.source_value for item in evidence if item.precedence == narrowest})
    )
    tiers = {TIER_BY_SECTION.get(label, "unknown") for label in labels}
    return (next(iter(tiers)) if len(tiers) == 1 else "unknown"), labels


def _route(record) -> str:
    if "/pokalwettbewerb/" in record.source_url:
        return "pokal"
    if "/wettbewerb/" in record.source_url:
        return "wettbewerb"
    return "unknown"


def _name_exclusion(record) -> str | None:
    named = [item for item in record.evidence if item.origin is EvidenceOrigin.NAME]
    women = record.gender in {Gender.WOMEN, Gender.MIXED} or any(
        item.gender in {Gender.WOMEN, Gender.MIXED} for item in named
    )
    if women:
        return "excluded_women"
    reserve = record.team_type in {TeamType.RESERVE, TeamType.MIXED} or any(
        item.team_type in {TeamType.RESERVE, TeamType.MIXED} for item in named
    )
    if reserve:
        return "reserve"
    youth = record.age_category in {AgeCategory.YOUTH, AgeCategory.UXX} or any(
        item.age_category in {AgeCategory.YOUTH, AgeCategory.UXX} for item in named
    )
    if youth or record.competition_id in YOUTH_SPECIAL_IDS:
        return "youth"
    status = record.classification_status.value
    return None if status == "eligible" else status


def _live_decision(record, edition) -> bool | None:
    try:
        year = season_window_year(
            edition.edition_id, edition.season_format, edition.canonical_season
        )
    except (TypeError, ValueError):
        return None
    if isinstance(year, bool) or not isinstance(year, int):
        return None
    threshold = 2022 if record.team_type is TeamType.NATIONAL_TEAM else 2025
    return year >= threshold


def _live_class(record, tier: str, route: str, country: str) -> str:
    if record.team_type is TeamType.NATIONAL_TEAM:
        return "core_national"
    if record.team_type is not TeamType.CLUB or route == "unknown" or tier == "unknown":
        return "unknown"
    if tier in {"5", "6"} or record.competition_id in AMATEUR_SPECIAL_IDS:
        return "amateur"
    if country == "Germany" and record.name.startswith("Landespokal "):
        return "amateur"
    if tier == "regional" and country in CONTINENT_GEOGRAPHY:
        return "unknown"
    if tier == "regional" and country != "Brazil":
        return "amateur"
    return "core_club"


def _entry(raw: Mapping[str, Any], title_only_ids: frozenset[str]) -> UniverseEntry:
    record = _competition_from_joined_row(raw)
    edition = _edition_from_joined_row(raw)
    exclusion = _name_exclusion(record)
    tier, labels = _tier(record)
    country = record.country
    country_evidence = None
    proof = PROFILE_COUNTRIES.get(
        (record.competition_id, record.source_url, edition.source_body_hash)
    )
    if proof and tier == "regional":
        region, profile_country = proof
        if country == region and record.confederation == region:
            country = profile_country
            country_evidence = edition.source_body_hash
        elif country != profile_country:
            country = "Unknown"
    route = _route(record)
    live = _live_decision(record, edition)
    subtype = None
    if exclusion is not None:
        competition_class = exclusion
        if exclusion in {"youth", "reserve"} and live is False:
            subtype = exclusion
            competition_class = "archive"
    elif live is None:
        competition_class = "unknown"
    else:
        base_class = _live_class(record, tier, route, country)
        if live:
            competition_class = base_class
        else:
            subtype = base_class
            competition_class = "archive"
    reason = (
        f"registry:{record.classification_status.value};tier:{tier};"
        f"live:{'unknown' if live is None else str(live).lower()}"
    )
    row = DenominatorRow(
        competition_id=record.competition_id,
        name=record.name,
        country=country,
        confederation=record.confederation,
        tier=tier,
        competition_class=competition_class,
        route=route,
        live=live is True,
        current_saison_id=int(edition.edition_id),
        step2_saison_ids=(),
        fotmob_id="",
        sofascore_id="",
        fbref_id="",
        reason=reason,
    )
    return UniverseEntry(
        row=row,
        registry_status=str(
            raw.get("classification_status") or record.classification_status.value
        ),
        team_type=record.team_type.value,
        section_labels=labels,
        title_only=record.competition_id in title_only_ids,
        live_decision=live,
        class_subtype=subtype,
        country_evidence_sha256=country_evidence,
    )


def build_registry_denominator(
    registry_rows: Iterable[Mapping[str, Any]],
    *,
    quarantined: Mapping[str, str] | None = None,
    title_only_ids: Iterable[str] = (),
) -> RegistryDenominator:
    canonical, universe_version = _canonical_rows(registry_rows)
    quarantine = _quarantine_rows(quarantined)
    collisions = sorted(
        set(canonical) & {competition_id for competition_id, _ in quarantine}
    )
    if collisions:
        raise DenominatorError(f"published/quarantine collision: {collisions}")
    title_ids = _title_ids(title_only_ids)
    entries = []
    for competition_id, raw in sorted(canonical.items()):
        try:
            entries.append(_entry(raw, frozenset(title_ids)))
        except (TypeError, ValueError) as exc:
            raise DenominatorError(
                f"{competition_id}: invalid registry row: {exc}"
            ) from exc
    entries = tuple(entries)
    return RegistryDenominator(
        denominator=Denominator(
            rows={entry.row.competition_id: entry.row for entry in entries}
        ),
        universe_version=universe_version,
        entries=entries,
        quarantined=quarantine,
        title_only_ids=title_ids,
    )


def _legacy_audit(row: DenominatorRow, version: str) -> dict[str, Any]:
    team_type = (
        "national_team"
        if row.competition_class == "core_national"
        else "reserve"
        if row.competition_class == "reserve"
        else "club"
    )
    return {
        "membership": row.is_core,
        "competition_class": row.competition_class,
        "class_subtype": None,
        "team_type": team_type,
        "tier": row.tier,
        "live_decision": row.live,
        "current_edition": str(row.current_saison_id),
        "version": version,
        "title_only": False,
    }


def _entry_audit(entry: UniverseEntry, version: str) -> dict[str, Any]:
    return {
        "membership": entry.row.is_core,
        "competition_class": entry.row.competition_class,
        "class_subtype": entry.class_subtype,
        "team_type": entry.team_type,
        "tier": entry.row.tier,
        "live_decision": entry.live_decision,
        "current_edition": str(entry.row.current_saison_id),
        "version": version,
        "title_only": entry.title_only,
        "country": entry.row.country,
        "country_evidence_sha256": entry.country_evidence_sha256,
    }


def diff_status(report: Mapping[str, Any]) -> dict[str, Any]:
    delta = list(report.get("denominator_delta", ()))
    unexplained = [
        str(row.get("competition_id", ""))
        for row in delta
        if not row.get("explanations")
    ]
    missing = set(report.get("legacy_core_quarantine_ids", ()))
    missing.update(report.get("legacy_core_publication_absence_ids", ()))
    missing.update(
        row["competition_id"]
        for row in delta
        if row.get("new") is None
        and ((row.get("old") or {}).get("membership") or row.get("change") == "removed")
    )
    unresolved = [
        str(row.get("competition_id", ""))
        for row in delta
        if (row.get("new") or {}).get("competition_class") == "unknown"
    ]
    complete = not unexplained and not missing
    return {
        "complete": complete,
        "migration_clean": complete and not unresolved,
        "missing_legacy_core_ids": sorted(missing),
        "unexplained": unexplained,
    }


def compare_denominators(
    legacy: Denominator,
    current: RegistryDenominator,
    *,
    legacy_version: str,
) -> dict[str, Any]:
    if not legacy_version.strip() or not current.universe_version.strip():
        raise DenominatorError("legacy and registry versions are required")
    entries = {entry.row.competition_id: entry for entry in current.entries}
    legacy_ids = legacy.denominator_ids()
    registry_ids = current.denominator.denominator_ids()
    quarantine_ids = {competition_id for competition_id, _ in current.quarantined}
    legacy_core_quarantine = sorted(legacy_ids & quarantine_ids)
    delta = []
    for competition_id in sorted(legacy_ids | registry_ids):
        old_row = legacy.rows.get(competition_id)
        new_entry = entries.get(competition_id)
        old = _legacy_audit(old_row, legacy_version) if old_row else None
        new = _entry_audit(new_entry, current.universe_version) if new_entry else None
        old_member = competition_id in legacy_ids
        new_member = competition_id in registry_ids
        if (
            old
            and new
            and all(
                old[key] == new[key]
                for key in ("competition_class", "team_type", "tier", "current_edition")
            )
        ):
            continue
        explanations = []
        if old_row is None:
            explanations.append("publication_presence")
        elif new_entry is None:
            explanations.append(
                "quarantine"
                if competition_id in quarantine_ids
                else "publication_absence"
            )
        if old and new:
            if old["competition_class"] != new["competition_class"]:
                explanations.append("classification")
            if old["team_type"] != new["team_type"]:
                explanations.append("team_type")
            if old["tier"] != new["tier"]:
                explanations.append("tier")
            if old["live_decision"] != new["live_decision"]:
                explanations.append("live_filter")
            if old["current_edition"] != new["current_edition"]:
                explanations.append("current_edition")
            if new["competition_class"] == "unknown" and new["tier"] == "regional":
                explanations.append("geography")
            elif new_entry.country_evidence_sha256:
                explanations.append("geography")
        if new_entry and new_entry.title_only:
            explanations.append("title_only")
        if not explanations:
            explanations.append("metadata")
        change = (
            "added"
            if not old_member and new_member
            else "removed"
            if old_member and not new_member
            else "changed"
        )
        delta.append(
            {
                "competition_id": competition_id,
                "change": change,
                "old": old,
                "new": new,
                "explanations": sorted(set(explanations)),
            }
        )
    report = {
        "legacy_version": legacy_version,
        "universe_version": current.universe_version,
        "legacy_competition_count": len(legacy.rows),
        "registry_competition_count": len(entries),
        "legacy_denominator_count": len(legacy_ids),
        "registry_denominator_count": len(registry_ids),
        "legacy_class_counts": dict(
            sorted(
                Counter(row.competition_class for row in legacy.rows.values()).items()
            )
        ),
        "registry_class_counts": dict(
            sorted(
                Counter(
                    entry.row.competition_class for entry in current.entries
                ).items()
            )
        ),
        "denominator_delta": delta,
        "excluded": [
            {
                "competition_id": competition_id,
                **_entry_audit(entry, current.universe_version),
            }
            for competition_id, entry in sorted(entries.items())
            if entry.registry_status != "eligible"
        ],
        "title_only": [
            {
                "competition_id": competition_id,
                "present": competition_id in entries,
                "quarantined": competition_id in quarantine_ids,
                **(
                    _entry_audit(entries[competition_id], current.universe_version)
                    if competition_id in entries
                    else {}
                ),
            }
            for competition_id in current.title_only_ids
        ],
        "quarantined": [
            {"competition_id": competition_id, "reason": reason}
            for competition_id, reason in current.quarantined
        ],
        "legacy_core_quarantine_ids": legacy_core_quarantine,
        "legacy_core_publication_absence_ids": sorted(
            legacy_ids - entries.keys() - quarantine_ids
        ),
    }
    report.update(diff_status(report))
    return report


__all__ = [
    "ARCHIVE_SECTION",
    "TIER_BY_SECTION",
    "RegistryDenominator",
    "UniverseEntry",
    "build_registry_denominator",
    "compare_denominators",
    "current_denominator_snapshot_id",
    "denominator_source",
    "diff_status",
    "registry_snapshot_id",
]
