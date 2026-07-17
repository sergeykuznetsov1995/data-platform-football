"""Auditable FotMob entity/field coverage registry.

FotMob does not publish a versioned schema.  The raw layer therefore preserves
the complete JSON document and records every observed JSON path.  This module
classifies paths into one of three explicit dispositions:

``typed``
    projected into canonical columns/rows;
``raw_only``
    deliberately retained only in the content-addressed raw object (and, for
    important fragments, a ``*_json`` column);
``excluded``
    intentionally not modelled, with a human-readable reason.

An observed path that matches no rule is schema drift.  It remains safely in
raw storage, but a run must not silently publish it through a legacy current
projection until the registry/parser is reviewed.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Mapping, Sequence


class FieldDisposition(str, Enum):
    TYPED = "typed"
    RAW_ONLY = "raw_only"
    EXCLUDED = "excluded"


@dataclass(frozen=True)
class FieldRule:
    pattern: str
    disposition: FieldDisposition
    entity: str
    reason: str

    def matches(self, path: str) -> bool:
        """Match an exact path or a trailing ``.*`` subtree rule."""

        if self.pattern == ".*":
            return True
        if self.pattern.endswith(".*"):
            root = self.pattern[:-2]
            return path == root or path.startswith(root + ".") or path.startswith(
                root + "[]"
            )
        return path == self.pattern


@dataclass(frozen=True)
class FieldCoverage:
    typed: tuple[str, ...]
    raw_only: tuple[str, ...]
    excluded: tuple[str, ...]
    unknown: tuple[str, ...]

    @property
    def has_schema_drift(self) -> bool:
        return bool(self.unknown)


# Registry is intentionally organized around API targets, not old table names.
# Array indices are normalized by parsers to ``[]``.
FIELD_RULES: Mapping[str, tuple[FieldRule, ...]] = {
    "all_leagues": (
        FieldRule("", FieldDisposition.RAW_ONLY, "catalog_raw", "JSON document root"),
        FieldRule("countries.*", FieldDisposition.RAW_ONLY, "catalog_raw", "catalog grouping/container metadata"),
        FieldRule("countries[].ccode", FieldDisposition.TYPED, "competitions", "country code"),
        FieldRule("countries[].name", FieldDisposition.TYPED, "competitions", "country label"),
        FieldRule("countries[].leagues[].id", FieldDisposition.TYPED, "competitions", "numeric source identity"),
        FieldRule("countries[].leagues[].name", FieldDisposition.TYPED, "competitions", "source label"),
        FieldRule("countries[].leagues[].pageUrl", FieldDisposition.TYPED, "competitions", "source route"),
        FieldRule("countries[].leagues[].localizedNameId", FieldDisposition.RAW_ONLY, "competitions", "localization token retained raw"),
        FieldRule("countries[].leagues[].isPopular", FieldDisposition.TYPED, "competitions", "discovery presentation flag"),
        FieldRule("popularLeagues.*", FieldDisposition.RAW_ONLY, "competitions", "duplicate presentation list; numeric ids deduplicated against countries"),
        FieldRule("popular.*", FieldDisposition.RAW_ONLY, "competitions", "duplicate presentation list; numeric ids deduplicated against countries"),
        FieldRule("international.*", FieldDisposition.RAW_ONLY, "competitions", "variant index grouping retained raw"),
    ),
    "league_season": (
        FieldRule("", FieldDisposition.RAW_ONLY, "season_raw", "JSON document root"),
        FieldRule("details.*", FieldDisposition.TYPED, "competition_seasons", "season identity and source metadata"),
        FieldRule("seostr", FieldDisposition.TYPED, "competition_seasons", "source presentation slug"),
        FieldRule("QAData.*", FieldDisposition.RAW_ONLY, "season_raw", "source QA/debug payload retained verbatim"),
        FieldRule("allAvailableSeasons[]", FieldDisposition.TYPED, "competition_seasons", "exact source season keys"),
        FieldRule("allAvailableSeasons", FieldDisposition.TYPED, "competition_seasons", "source season list container"),
        FieldRule("fixtures.*", FieldDisposition.TYPED, "matches", "fixtures, statuses, stages and fixture team universe"),
        FieldRule("table.*", FieldDisposition.TYPED, "standings", "all table contexts, groups and zero-valued points"),
        FieldRule("playoff.*", FieldDisposition.TYPED, "season_stages", "qualification and knockout structure"),
        FieldRule("stats", FieldDisposition.RAW_ONLY, "season_raw", "statistics container"),
        FieldRule("stats.players.*", FieldDisposition.TYPED, "leaderboard_categories", "advertised player categories"),
        FieldRule("stats.teams.*", FieldDisposition.TYPED, "leaderboard_categories", "advertised team categories"),
        FieldRule("stats.seasonStatLinks.*", FieldDisposition.TYPED, "competition_seasons", "exact season/stat-stage links"),
        FieldRule("stats.seasonsWithLinks.*", FieldDisposition.TYPED, "competition_seasons", "exact seasons with statistics"),
        FieldRule("transfers.*", FieldDisposition.RAW_ONLY, "transfer_capability", "league payload descriptor; events come from paginated endpoint"),
        FieldRule("overview.*", FieldDisposition.RAW_ONLY, "season_raw", "presentation widgets preserved raw"),
        FieldRule("tabs.*", FieldDisposition.RAW_ONLY, "season_raw", "UI navigation metadata"),
        FieldRule("seasons.*", FieldDisposition.RAW_ONLY, "season_raw", "secondary source season representation"),
        FieldRule("seasons[].seasonName", FieldDisposition.TYPED, "competition_seasons", "historical season identity"),
        FieldRule("seasons[].winner.*", FieldDisposition.TYPED, "competition_seasons", "historical champion"),
        FieldRule("seasons[].loser.*", FieldDisposition.TYPED, "competition_seasons", "historical runner-up"),
    ),
    "leaderboard": (
        FieldRule("", FieldDisposition.RAW_ONLY, "leaderboard_raw", "JSON document root"),
        FieldRule("TopLists.*", FieldDisposition.RAW_ONLY, "leaderboard_raw", "leaderboard list/container metadata"),
        FieldRule("TopLists[].Title", FieldDisposition.TYPED, "leaderboards", "list title"),
        FieldRule("TopLists[].Category", FieldDisposition.TYPED, "leaderboards", "category"),
        FieldRule("TopLists[].StatName", FieldDisposition.TYPED, "leaderboards", "stat identity"),
        FieldRule("TopLists[].StatList[].*", FieldDisposition.TYPED, "leaderboards", "all advertised leaderboard rows"),
        FieldRule("TopLists[].Subtitle", FieldDisposition.RAW_ONLY, "leaderboards", "presentation text"),
        FieldRule("LeagueName", FieldDisposition.RAW_ONLY, "leaderboard_raw", "display label; competition identity comes from the requesting scope"),
    ),
    "transfers": (
        FieldRule("", FieldDisposition.RAW_ONLY, "transfers_raw", "JSON document root"),
        FieldRule("hits", FieldDisposition.TYPED, "transfer_events", "pagination completeness total"),
        FieldRule("page", FieldDisposition.TYPED, "transfer_events", "source page"),
        FieldRule("pageSize", FieldDisposition.TYPED, "transfer_events", "source page size"),
        FieldRule("transfers[].*", FieldDisposition.TYPED, "transfer_events", "global transfer event including feeText/localizedFeeText/value"),
        FieldRule("transfers.*", FieldDisposition.RAW_ONLY, "transfers_raw", "transfer list/container metadata"),
        # Appeared live on 2026-07-17 (isolated-contour acceptance run): a
        # top-level fee-filter bound for the page's slider UI, not a transfer
        # event attribute.
        FieldRule("maxFee", FieldDisposition.RAW_ONLY, "transfers_raw", "fee filter bound; page chrome around the transfer stream"),
    ),
    "match": (
        FieldRule("", FieldDisposition.RAW_ONLY, "match_raw", "JSON document root"),
        FieldRule("content", FieldDisposition.RAW_ONLY, "match_raw", "match content container"),
        FieldRule("general.*", FieldDisposition.TYPED, "match_payloads", "match identity, teams and status"),
        FieldRule("header.*", FieldDisposition.TYPED, "match_payloads", "score and competition metadata"),
        FieldRule("content.matchFacts.*", FieldDisposition.TYPED, "match_payloads", "facts, referee and events"),
        FieldRule("content.stats.*", FieldDisposition.TYPED, "match_payloads", "team statistics"),
        FieldRule("content.playerStats.*", FieldDisposition.TYPED, "match_payloads", "player statistics"),
        FieldRule("content.lineup.*", FieldDisposition.TYPED, "match_payloads", "lineups and benches"),
        FieldRule("content.shotmap.*", FieldDisposition.TYPED, "match_payloads", "shots"),
        FieldRule("content.h2h.*", FieldDisposition.RAW_ONLY, "match_payloads", "head-to-head context retained as JSON"),
        FieldRule("content.momentum.*", FieldDisposition.TYPED, "match_payloads", "momentum series"),
        FieldRule("content.buzz.*", FieldDisposition.EXCLUDED, "match_payloads", "ephemeral UI engagement widget; retained in raw object"),
        FieldRule("content.liveticker.*", FieldDisposition.RAW_ONLY, "match_raw", "live-ticker widget config; matches are ingested finished"),
        FieldRule("content.superlive.*", FieldDisposition.RAW_ONLY, "match_raw", "SuperLive stream widget config"),
        FieldRule("content.table.*", FieldDisposition.RAW_ONLY, "match_raw", "embedded league table; duplicate of competition standings ingestion"),
        FieldRule("content.hasPlayoff", FieldDisposition.RAW_ONLY, "match_raw", "UI flag mirroring competition playoff structure"),
        FieldRule("content.attackingZones.*", FieldDisposition.RAW_ONLY, "match_raw", "attacking-zones widget retained raw pending explicit modelling"),
        FieldRule("content.highlightStories.*", FieldDisposition.RAW_ONLY, "match_raw", "video highlight stories widget; assets with viewing restrictions, not match data"),
        FieldRule("content.heatmapUrl", FieldDisposition.RAW_ONLY, "match_raw", "external heatmap asset link"),
        FieldRule("content.weather.*", FieldDisposition.RAW_ONLY, "match_raw", "kickoff weather widget retained raw pending explicit modelling"),
        FieldRule("seo.*", FieldDisposition.RAW_ONLY, "match_raw", "SEO/JSON-LD page markup"),
        FieldRule("nav.*", FieldDisposition.RAW_ONLY, "match_raw", "UI navigation metadata"),
        FieldRule("hasPendingVAR", FieldDisposition.RAW_ONLY, "match_raw", "live VAR UI flag; matches are ingested finished"),
        FieldRule("ongoing", FieldDisposition.RAW_ONLY, "match_raw", "live-state UI flag; matches are ingested finished"),
    ),
    "team": (
        FieldRule("", FieldDisposition.RAW_ONLY, "team_raw", "JSON document root"),
        FieldRule("details.*", FieldDisposition.TYPED, "team_snapshots", "team source identity/profile"),
        FieldRule("overview.*", FieldDisposition.TYPED, "team_snapshots", "venue and current overview"),
        FieldRule("squad.*", FieldDisposition.TYPED, "squad_snapshots", "observed roster; never labelled historical"),
        FieldRule("history.*", FieldDisposition.RAW_ONLY, "team_snapshots", "team history retained as JSON"),
        FieldRule("fixtures.*", FieldDisposition.EXCLUDED, "team_snapshots", "duplicate of competition fixture ingestion"),
        FieldRule("QAData.*", FieldDisposition.RAW_ONLY, "team_raw", "source QA/debug payload retained verbatim"),
        FieldRule("allAvailableSeasons.*", FieldDisposition.RAW_ONLY, "team_raw", "team-page season list; duplicate of competition season ingestion"),
        FieldRule("seostr", FieldDisposition.RAW_ONLY, "team_raw", "source presentation slug"),
        FieldRule("stats.*", FieldDisposition.RAW_ONLY, "team_raw", "team-page leaderboard widgets; duplicate of competition leaderboard ingestion"),
        FieldRule("table.*", FieldDisposition.RAW_ONLY, "team_raw", "embedded league table; duplicate of competition standings ingestion"),
        FieldRule("tabs.*", FieldDisposition.RAW_ONLY, "team_raw", "UI navigation metadata"),
        FieldRule("transfers.*", FieldDisposition.RAW_ONLY, "team_raw", "team-page transfer widget; events come from the paginated endpoint"),
    ),
    "player": (
        FieldRule("", FieldDisposition.RAW_ONLY, "player_raw", "JSON document root"),
        FieldRule("props.*", FieldDisposition.RAW_ONLY, "player_raw", "Next.js props container metadata"),
        FieldRule("props.pageProps.*", FieldDisposition.TYPED, "player_snapshots", "profile, career, traits, trophies and market values"),
        FieldRule("pageProps.*", FieldDisposition.TYPED, "player_snapshots", "alternate Next payload root"),
        FieldRule("__N_SSP", FieldDisposition.RAW_ONLY, "player_raw", "Next.js framework flag"),
        FieldRule("buildId", FieldDisposition.RAW_ONLY, "player_raw", "Next.js build identity"),
        FieldRule("isFallback", FieldDisposition.RAW_ONLY, "player_raw", "Next.js framework flag"),
        FieldRule("gsp", FieldDisposition.RAW_ONLY, "player_raw", "Next.js framework flag"),
        FieldRule("context.*", FieldDisposition.RAW_ONLY, "player_raw", "Next.js request context (locale/path/user)"),
        FieldRule("toggles.*", FieldDisposition.RAW_ONLY, "player_raw", "feature-flag payload"),
        FieldRule("translations.*", FieldDisposition.RAW_ONLY, "player_raw", "localization string bundle"),
        FieldRule("url", FieldDisposition.RAW_ONLY, "player_raw", "page route echo"),
    ),
}


INTENTIONAL_EXCLUSIONS: tuple[dict[str, str], ...] = tuple(
    {
        "target_type": target_type,
        "path": rule.pattern,
        "entity": rule.entity,
        "reason": rule.reason,
    }
    for target_type, rules in FIELD_RULES.items()
    for rule in rules
    if rule.disposition == FieldDisposition.EXCLUDED
)


def classify_paths(
    target_type: str,
    observed_paths: Iterable[str],
    *,
    rules: Mapping[str, Sequence[FieldRule]] = FIELD_RULES,
) -> FieldCoverage:
    """Classify every observed path; unknown paths are never discarded."""

    configured = tuple(rules.get(target_type, ()))
    typed: list[str] = []
    raw_only: list[str] = []
    excluded: list[str] = []
    unknown: list[str] = []
    normalized_paths = {
        (str(item)[2:] if str(item).startswith("$.") else str(item).lstrip("$"))
        for item in observed_paths
    }
    for path in sorted(normalized_paths):
        matches = [rule for rule in configured if rule.matches(path)]
        if not matches:
            unknown.append(path)
            continue
        # Most specific rule wins; the catch-all ``.*`` cannot hide a typed or
        # intentionally excluded subtree.
        rule = max(matches, key=lambda item: len(item.pattern.rstrip("*")))
        destination = {
            FieldDisposition.TYPED: typed,
            FieldDisposition.RAW_ONLY: raw_only,
            FieldDisposition.EXCLUDED: excluded,
        }[rule.disposition]
        destination.append(path)
    return FieldCoverage(
        typed=tuple(typed),
        raw_only=tuple(raw_only),
        excluded=tuple(excluded),
        unknown=tuple(unknown),
    )


def entity_map() -> dict[str, dict[str, object]]:
    """Return a serializable source entity/field disposition map."""

    return {
        target_type: {
            "rules": [
                {
                    "path": rule.pattern,
                    "disposition": rule.disposition.value,
                    "entity": rule.entity,
                    "reason": rule.reason,
                }
                for rule in rules
            ]
        }
        for target_type, rules in FIELD_RULES.items()
    }


__all__ = [
    "FIELD_RULES",
    "INTENTIONAL_EXCLUSIONS",
    "FieldCoverage",
    "FieldDisposition",
    "FieldRule",
    "classify_paths",
    "entity_map",
]
