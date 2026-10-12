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
    columns: tuple[str, ...] = ()

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
        FieldRule("countries[].ccode", FieldDisposition.TYPED, "competitions", "country code", ("fotmob_competitions.country_code",)),
        FieldRule("countries[].name", FieldDisposition.TYPED, "competitions", "country label", ("fotmob_competitions.country_name",)),
        FieldRule("countries[].leagues[].id", FieldDisposition.TYPED, "competitions", "numeric source identity", ("fotmob_competitions.competition_id",)),
        FieldRule("countries[].leagues[].name", FieldDisposition.TYPED, "competitions", "source label", ("fotmob_competitions.name",)),
        FieldRule("countries[].leagues[].pageUrl", FieldDisposition.TYPED, "competitions", "source route", ("fotmob_competitions.page_url",)),
        FieldRule("countries[].leagues[].localizedNameId", FieldDisposition.RAW_ONLY, "competitions", "localization token retained raw"),
        FieldRule("countries[].leagues[].isPopular", FieldDisposition.RAW_ONLY, "competitions", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("popularLeagues.*", FieldDisposition.RAW_ONLY, "competitions", "duplicate presentation list; numeric ids deduplicated against countries"),
        FieldRule("popular.*", FieldDisposition.RAW_ONLY, "competitions", "duplicate presentation list; numeric ids deduplicated against countries"),
        FieldRule("international.*", FieldDisposition.RAW_ONLY, "competitions", "variant index grouping retained raw"),
    ),
    "competition_profile": (
        FieldRule("", FieldDisposition.RAW_ONLY, "competition_profile_raw", "JSON document root"),
        FieldRule("details.*", FieldDisposition.RAW_ONLY, "competition_scope_observations", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule(".*", FieldDisposition.RAW_ONLY, "competition_profile_raw", "season and presentation data is retained raw for later fan-out"),
    ),
    "league_season": (
        FieldRule("", FieldDisposition.RAW_ONLY, "season_raw", "JSON document root"),
        FieldRule("details.*", FieldDisposition.RAW_ONLY, "competition_seasons", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("seostr", FieldDisposition.TYPED, "competition_seasons", "source presentation slug", ("fotmob_competition_seasons.source_slug",)),
        FieldRule("QAData.*", FieldDisposition.RAW_ONLY, "season_raw", "source QA/debug payload retained verbatim"),
        FieldRule("allAvailableSeasons[]", FieldDisposition.TYPED, "competition_seasons", "exact source season keys", ("fotmob_competition_seasons.source_season_key",)),
        FieldRule("allAvailableSeasons", FieldDisposition.RAW_ONLY, "competition_seasons", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("fixtures.*", FieldDisposition.RAW_ONLY, "matches", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("table.*", FieldDisposition.RAW_ONLY, "standings", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("playoff.*", FieldDisposition.RAW_ONLY, "season_stages", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("stats", FieldDisposition.RAW_ONLY, "season_raw", "statistics container"),
        FieldRule("stats.players.*", FieldDisposition.RAW_ONLY, "leaderboard_categories", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("stats.teams.*", FieldDisposition.RAW_ONLY, "leaderboard_categories", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("stats.seasonStatLinks.*", FieldDisposition.RAW_ONLY, "season_raw", "stat links may span phase seasons; no guaranteed season-row projection, retained raw"),
        FieldRule("stats.seasonsWithLinks.*", FieldDisposition.RAW_ONLY, "competition_seasons", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("transfers.*", FieldDisposition.RAW_ONLY, "transfer_capability", "league payload descriptor; events come from paginated endpoint"),
        FieldRule("overview.*", FieldDisposition.RAW_ONLY, "season_raw", "presentation widgets preserved raw"),
        FieldRule("tabs.*", FieldDisposition.RAW_ONLY, "season_raw", "UI navigation metadata"),
        FieldRule("seasons.*", FieldDisposition.RAW_ONLY, "season_raw", "secondary source season representation"),
        FieldRule("seasons[].seasonName", FieldDisposition.TYPED, "competition_seasons", "historical season identity", ("fotmob_competition_season_history.history_season_label",)),
        FieldRule("seasons[].winner.*", FieldDisposition.TYPED, "competition_seasons", "historical champion", ("fotmob_competition_season_history.season_history_json",)),
        FieldRule("seasons[].loser.*", FieldDisposition.TYPED, "competition_seasons", "historical runner-up", ("fotmob_competition_season_history.season_history_json",)),
    ),
    "leaderboard": (
        FieldRule("", FieldDisposition.RAW_ONLY, "leaderboard_raw", "JSON document root"),
        FieldRule("TopLists.*", FieldDisposition.RAW_ONLY, "leaderboard_raw", "leaderboard list/container metadata"),
        FieldRule("TopLists[].Title", FieldDisposition.TYPED, "leaderboards", "list title", ("fotmob_leaderboards.stat_category_header",)),
        FieldRule("TopLists[].Category", FieldDisposition.TYPED, "leaderboards", "category", ("fotmob_leaderboards.stat_category_group",)),
        FieldRule("TopLists[].StatName", FieldDisposition.TYPED, "leaderboards", "stat identity", ("fotmob_leaderboards.stat_name",)),
        FieldRule("TopLists[].StatList[].*", FieldDisposition.RAW_ONLY, "leaderboards", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("TopLists[].Subtitle", FieldDisposition.RAW_ONLY, "leaderboards", "presentation text"),
        FieldRule("LeagueName", FieldDisposition.RAW_ONLY, "leaderboard_raw", "display label; competition identity comes from the requesting scope"),
    ),
    "transfers": (
        FieldRule("", FieldDisposition.RAW_ONLY, "transfers_raw", "JSON document root"),
        FieldRule("hits", FieldDisposition.RAW_ONLY, "transfer_events", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("page", FieldDisposition.RAW_ONLY, "transfer_events", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("pageSize", FieldDisposition.RAW_ONLY, "transfer_events", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("transfers[].*", FieldDisposition.RAW_ONLY, "transfer_events", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("transfers.*", FieldDisposition.RAW_ONLY, "transfers_raw", "transfer list/container metadata"),
        # Appeared live on 2026-07-17 (isolated-contour acceptance run): a
        # top-level fee-filter bound for the page's slider UI, not a transfer
        # event attribute.
        FieldRule("maxFee", FieldDisposition.RAW_ONLY, "transfers_raw", "fee filter bound; page chrome around the transfer stream"),
        FieldRule("minAge", FieldDisposition.RAW_ONLY, "transfers_raw", "minimum age filter bound; retained in the raw transfer page"),
        FieldRule("minDate", FieldDisposition.RAW_ONLY, "transfers_raw", "minimum date filter bound; retained in the raw transfer page"),
        FieldRule("maxMarketValue", FieldDisposition.RAW_ONLY, "transfers_raw", "market value filter bound; retained in the raw transfer page"),
    ),
    "match": (
        FieldRule("", FieldDisposition.RAW_ONLY, "match_raw", "JSON document root"),
        FieldRule("content", FieldDisposition.RAW_ONLY, "match_raw", "match content container"),
        FieldRule("general.*", FieldDisposition.RAW_ONLY, "match_payloads", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("header.*", FieldDisposition.RAW_ONLY, "match_payloads", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("content.matchFacts.*", FieldDisposition.TYPED, "match_payloads", "facts, referee and events", ("fotmob_match_payloads.match_facts_json",)),
        FieldRule("content.stats.*", FieldDisposition.TYPED, "match_payloads", "team statistics", ("fotmob_match_payloads.stats_json",)),
        FieldRule("content.playerStats.*", FieldDisposition.TYPED, "match_payloads", "player statistics", ("fotmob_match_payloads.player_stats_json",)),
        FieldRule("content.lineup.*", FieldDisposition.TYPED, "match_payloads", "lineups and benches", ("fotmob_match_payloads.lineup_json",)),
        FieldRule("content.shotmap.*", FieldDisposition.TYPED, "match_payloads", "shots", ("fotmob_match_payloads.shotmap_json",)),
        FieldRule("content.h2h.*", FieldDisposition.RAW_ONLY, "match_payloads", "head-to-head context retained as JSON"),
        FieldRule("content.momentum.*", FieldDisposition.TYPED, "match_payloads", "momentum series", ("fotmob_match_payloads.momentum_json",)),
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
        FieldRule(".*", FieldDisposition.RAW_ONLY, "match_raw", "new matchDetails fields retained in raw and payload_json; specific rules take precedence"),
    ),
    "team": (
        FieldRule("", FieldDisposition.RAW_ONLY, "team_raw", "JSON document root"),
        FieldRule("details.*", FieldDisposition.TYPED, "team_snapshots", "team source identity/profile", ("fotmob_team_snapshots.details_json",)),
        FieldRule("overview.*", FieldDisposition.TYPED, "team_snapshots", "venue and current overview", ("fotmob_team_snapshots.overview_json",)),
        FieldRule("squad.*", FieldDisposition.RAW_ONLY, "squad_snapshots", "unprojected fields retained raw; explicit child projections are typed"),
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
        FieldRule("props.pageProps.*", FieldDisposition.RAW_ONLY, "player_snapshots", "unprojected fields retained raw; explicit child projections are typed"),
        FieldRule("pageProps.*", FieldDisposition.RAW_ONLY, "player_snapshots", "unprojected fields retained raw; explicit child projections are typed"),
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


# A wildcard is typed only when the entire fragment reaches a dedicated column.
# Other containers stay raw_only, with exact child projections listed below.
FIELD_PROJECTIONS = {
    "competition_profile": {
        "fotmob_competition_scope_observations": {
            "details.id": "competition_id",
            "details.name": "profile_name",
            "details.gender": "source_gender",
            "details.type": "source_type",
            "details.ageGroup": "source_age_group",
        },
    },
    "league_season": {
        "fotmob_competition_seasons": {
            "details.id": "competition_id",
            "details.selectedSeason": "source_season_key",
            "details.latestSeason": "is_latest",
        },
        "fotmob_matches": {
            "fixtures.allMatches[].id": "match_id",
            "fixtures.allMatches[].matchId": "match_id",
            "fixtures.allMatches[].status.utcTime": "utc_time",
            "fixtures.allMatches[].status.finished": "finished",
            "fixtures.allMatches[].status.started": "started",
            "fixtures.allMatches[].status.cancelled": "cancelled",
            "fixtures.allMatches[].status.awarded": "awarded",
            "fixtures.allMatches[].status.scoreStr": "score_text",
            "fixtures.allMatches[].status.reason.short": "status_reason_short",
            "fixtures.allMatches[].status.reason.long": "status_reason_long",
            "fixtures.allMatches[].home.id": "home_team_id",
            "fixtures.allMatches[].home.name": "home_team_name",
            "fixtures.allMatches[].away.id": "away_team_id",
            "fixtures.allMatches[].away.name": "away_team_name",
            "fixtures.allMatches[].round": "round_id",
            "fixtures.allMatches[].roundName": "round_name",
            "fixtures.allMatches[].stage": "stage_id",
            "fixtures.allMatches[].group": "group_name",
            "fixtures.allMatches[].pageUrl": "page_url",
        },
        "fotmob_leaderboard_categories": {
            "stats.players[].name": "name",
            "stats.players[].header": "header",
            "stats.players[].category": "category",
            "stats.players[].fetchAllUrl": "fetch_all_url",
            "stats.players[].localizedTitleId": "localized_title_id",
            "stats.teams[].name": "name",
            "stats.teams[].header": "header",
            "stats.teams[].category": "category",
            "stats.teams[].fetchAllUrl": "fetch_all_url",
            "stats.teams[].localizedTitleId": "localized_title_id",
        },
    },
    "leaderboard": {
        "fotmob_leaderboards": {
            "TopLists[].StatList[].ParticipantId": "participant_id",
            "TopLists[].StatList[].ParticipantName": "participant_name",
            "TopLists[].StatList[].TeamId": "team_id",
            "TopLists[].StatList[].TeamName": "team_name",
            "TopLists[].StatList[].TeamColor": "team_color",
            "TopLists[].StatList[].ParticipantCountryCode": "country_code",
            "TopLists[].StatList[].Rank": "rank",
            "TopLists[].StatList[].StatValue": "stat_value",
            "TopLists[].StatList[].SubStatValue": "sub_stat_value",
            "TopLists[].StatList[].StatValueCount": "stat_value_count",
            "TopLists[].StatList[].MatchesPlayed": "matches_played",
            "TopLists[].StatList[].MinutesPlayed": "minutes_played",
        },
    },
    "transfers": {
        "fotmob_transfer_events": {
            "transfers[].playerId": "player_id",
            "transfers[].name": "player_name",
            "transfers[].transferDate": "transfer_date",
            "transfers[].feeText": "fee_text",
            "transfers[].localizedFeeText": "localized_fee_text",
            "transfers[].value": "fee_value",
            "transfers[].onLoan": "on_loan",
            "transfers[].fromClub.id": "from_club_id",
            "transfers[].toClub.id": "to_club_id",
            "transfers[].position.label": "position_label",
            "transfers[].position.key": "position_key",
            "transfers[].transferType.localizationKey": "transfer_type_key",
            "transfers[].transferType.text": "transfer_type_text",
        },
    },
    "match": {
        "fotmob_match_payloads": {"general.matchId": "match_id"},
    },
    "team": {
        "fotmob_squad_snapshots": {
            "squad.squad[].title": "section",
            "squad.squad[].role": "section",
            "squad.squad[].members[].*": "member_json",
        },
    },
    "player": {
        "fotmob_player_snapshots": {
            "pageProps.data.id": "player_id",
            "pageProps.data.name": "name",
            "pageProps.data.birthDate": "birth_date",
            "pageProps.data.isCoach": "is_coach",
            "pageProps.data.isCaptain": "is_captain",
            "pageProps.data.gender": "gender",
            "pageProps.data.primaryTeam.teamId": "primary_team_id",
            "pageProps.data.primaryTeam.teamName": "primary_team_name",
            "pageProps.data.mainLeague.leagueId": "main_league_id",
            "pageProps.data.mainLeague.leagueName": "main_league_name",
            "pageProps.data.playerInformation.*": "player_information_json",
            "pageProps.data.injuryInformation.*": "injury_information_json",
            "pageProps.data.trophies.*": "trophies_json",
            "pageProps.data.careerHistory.*": "career_history_json",
            "pageProps.data.statSeasons.*": "stat_seasons_json",
            "pageProps.data.firstSeasonStats.*": "first_season_stats_json",
            "pageProps.data.recentMatches.*": "recent_matches_json",
            "pageProps.data.marketValues.*": "market_values_json",
            "pageProps.data.traits.*": "traits_json",
            "pageProps.data.meta.*": "meta_json",
            "pageProps.data.coachStats.*": "coach_stats_json",
            "pageProps.data.nextMatch.*": "next_match_json",
        },
    },
}

FIELD_RULES = {
    target: rules + tuple(
        FieldRule(path, FieldDisposition.TYPED, table.removeprefix("fotmob_"),
                  "explicit parser projection", (f"{table}.{column}",))
        for table, projections in FIELD_PROJECTIONS.get(target, {}).items()
        for path, column in projections.items()
    )
    for target, rules in FIELD_RULES.items()
}
# The Next.js wrapper is an alternate root for the same player data.
FIELD_RULES["player"] += tuple(
    FieldRule("props." + rule.pattern, rule.disposition, rule.entity, rule.reason, rule.columns)
    for rule in FIELD_RULES["player"]
    if rule.disposition == FieldDisposition.TYPED
)


def projection_matrix(
    rules: Mapping[str, Sequence[FieldRule]] = FIELD_RULES,
) -> list[dict[str, object]]:
    """Return every registered rule with its destination or raw-only reason."""

    return [
        {"target_type": target, "path": rule.pattern,
         "disposition": rule.disposition.value, "columns": list(rule.columns),
         "reason": rule.reason}
        for target, configured in sorted(rules.items())
        for rule in sorted(configured, key=lambda item: item.pattern)
    ]


def validate_projections(
    rules: Mapping[str, Sequence[FieldRule]] = FIELD_RULES,
) -> tuple[str, ...]:
    """Fail closed if a typed path lacks a concrete table.column destination."""

    errors = []
    for target, configured in rules.items():
        seen = set()
        for rule in configured:
            label = f"{target}:{rule.pattern}"
            if rule.pattern in seen:
                errors.append(f"{label}: duplicate rule")
            seen.add(rule.pattern)
            if not rule.reason.strip():
                errors.append(f"{label}: missing disposition reason")
            if rule.disposition == FieldDisposition.TYPED and (
                not rule.columns or any(len(column.split(".")) != 2 or
                                        not all(column.split(".")) for column in rule.columns)
            ):
                errors.append(f"{label}: missing table.column projection")
    return tuple(errors)


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
                    "columns": list(rule.columns),
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
    "projection_matrix",
    "validate_projections",
]
