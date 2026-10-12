from scrapers.fotmob.field_map import (
    INTENTIONAL_EXCLUSIONS,
    FieldDisposition,
    FieldRule,
    classify_paths,
    entity_map,
    projection_matrix,
    validate_projections,
)


def test_1291_every_typed_rule_has_a_projection():
    for target_type, target in entity_map().items():
        for rule in target["rules"]:
            if rule["disposition"] == "typed":
                assert rule.get("columns"), (target_type, rule["path"])


def test_1291_match_fallback_does_not_hide_known_rules():
    result = classify_paths("match", ["newRoot.x", "content.newWidget", "content.stats.x", "content.buzz.x"])
    assert result.unknown == ()
    assert result.raw_only == ("content.newWidget", "newRoot.x")
    assert result.typed == ("content.stats.x",)
    assert result.excluded == ("content.buzz.x",)


def test_projection_checker_rejects_missing_destination_and_reason():
    errors = validate_projections({"broken": (
        FieldRule("field", FieldDisposition.TYPED, "entity", ""),
    )})
    assert len(errors) == 2
    assert any("table.column" in error for error in errors)
    assert validate_projections() == ()


def test_1291_unprojected_match_metadata_is_explicitly_raw_only():
    coverage = classify_paths("match", ["general.matchId", "general.customFlag", "header.teams[].name"])
    assert coverage.typed == ("general.matchId",)
    assert coverage.raw_only == ("general.customFlag", "header.teams[].name")


def test_phase_split_stat_links_are_raw_only_when_no_season_row_projects_them():
    from scrapers.fotmob.catalog import classify_competition, competition_from_league_payload
    from scrapers.fotmob.transport import canonicalize_target
    from tests.unit.scrapers.test_fotmob_service import _competition_payload, _service
    payload = _competition_payload(230, "Liga MX")
    payload["details"].update(selectedSeason="2026/2027 - Apertura", latestSeason="2026/2027 - Apertura")
    payload["allAvailableSeasons"] = ["2026/2027 - Apertura", "2025/2026 - Clausura"]
    payload["stats"]["seasonStatLinks"] = [{"Name": "2026/2027", "Group": "Apertura"}]
    service, _, repository = _service({canonicalize_target("leagues", {"id": 230}).canonical_url: payload})
    ref = competition_from_league_payload(payload)
    discovered = service.discover_competition(classify_competition(ref, ref))
    assert discovered.operation.ok
    assert all(row["stat_link_json"] is None for row in repository.tables["fotmob_competition_seasons"])
    coverage = classify_paths("league_season", ["stats.seasonStatLinks[].Group"])
    assert coverage.raw_only == ("stats.seasonStatLinks[].Group",)


def test_projection_columns_exist_in_real_parser_outputs():
    """Removing a declared column from a producer must break this audit."""
    from datetime import datetime, timezone
    from scrapers.fotmob.domain import ScopeRef
    from scrapers.fotmob.parsers import parse_leaderboards, parse_transfers
    from scrapers.fotmob.service import FotMobIngestService
    from scrapers.fotmob.transport import canonicalize_target
    from tests.unit.scrapers.test_fotmob_service import _competition_payload, _service

    profile = _competition_payload(47, "Premier League")
    profile["seasons"] = [{"seasonName": "2024/2025", "winner": {"id": 1}}]
    profile["seostr"] = "premier-league"
    index = {"countries": [{"ccode": "ENG", "name": "England", "leagues": [
        {"id": 47, "name": "Premier League", "pageUrl": "/leagues/47"},
    ]}]}
    service, _, repository = _service({
        canonicalize_target("allLeagues").canonical_url: index,
        canonicalize_target("leagues", {"id": 47}).canonical_url: profile,
        canonicalize_target("leagues", {"id": 47, "season": "2025/2026"}).canonical_url: profile,
    })
    catalog = service.discover_catalog()
    assert catalog.operation.ok
    discovered = service.discover_competitions(catalog.classifications, profile_payloads=catalog.profile_payloads)
    assert all(item.operation.ok for item in discovered)
    result, bundle = service.sync_season(47, "2025/2026")
    assert result.ok and bundle is not None
    tables = repository.tables
    scope = ScopeRef(47, "2025/2026")
    tables["fotmob_leaderboards"] = list(parse_leaderboards({"TopLists": [{"StatList": [{}]}]}, participant_type="player", scope=scope))
    tables["fotmob_transfer_events"] = list(parse_transfers([{"transfers": [{"playerId": 1}]}], scope=scope))
    observed = datetime(2026, 10, 12, tzinfo=timezone.utc)
    match, _ = FotMobIngestService._match_payload_row(bundle.matches[0], {
        "general": {"matchId": 100}, "content": {"matchFacts": {"events": []}},
    })
    tables["fotmob_match_payloads"] = [match]
    team, squad, _ = FotMobIngestService._team_rows(1, {
        "details": {"id": 1}, "squad": {"squad": [{"members": [{"id": 10}]}]},
    }, observed)
    tables["fotmob_team_snapshots"] = [team]
    tables["fotmob_squad_snapshots"] = squad
    tables["fotmob_player_snapshots"] = [FotMobIngestService._player_snapshot_row(10, {
        "pageProps": {"data": {"id": 10}},
    }, observed)]
    for entry in projection_matrix():
        for destination in entry["columns"]:
            table, column = destination.split(".")
            assert tables[table], (entry, "no producer rows")
            assert all(column in row for row in tables[table]), entry
    assert tables["fotmob_competitions"][0]["country_code"] == "ENG"
    assert tables["fotmob_matches"][0]["match_id"] == 100
    assert match["match_facts_json"] == {"events": []}


def test_specific_typed_rule_wins_over_raw_root_fallback():
    coverage = classify_paths(
        "league_season",
        ["details.selectedSeason", "overview.foo", "brandNewRoot.value"],
    )
    assert coverage.typed == ("details.selectedSeason",)
    assert coverage.raw_only == ("overview.foo",)
    assert coverage.unknown == ("brandNewRoot.value",)
    assert coverage.has_schema_drift


def test_unknown_path_is_schema_drift_when_no_catch_all_is_configured():
    coverage = classify_paths(
        "strict",
        ["known", "new.path"],
        rules={
            "strict": (
                FieldRule(
                    "known", FieldDisposition.TYPED, "entity", "known field"
                ),
            )
        },
    )
    assert coverage.typed == ("known",)
    assert coverage.unknown == ("new.path",)
    assert coverage.has_schema_drift


def test_transfer_filter_bounds_are_exact_raw_only_paths():
    paths = ["minAge", "minDate", "maxMarketValue"]
    coverage = classify_paths("transfers", paths)
    assert coverage.raw_only == tuple(sorted(paths))
    assert not coverage.unknown
    unknown = ["minAge.value", "minDate.value", "maxMarketValue.value", "newFilter"]
    assert classify_paths("transfers", unknown).unknown == tuple(sorted(unknown))
    rules = entity_map()["transfers"]["rules"]
    assert all(any(rule["path"] == path and rule["reason"] for rule in rules) for path in paths)


def test_intentional_exclusions_are_exported_with_reasons():
    assert INTENTIONAL_EXCLUSIONS
    assert all(item["reason"] for item in INTENTIONAL_EXCLUSIONS)
    assert any(item["path"] == "content.buzz.*" for item in INTENTIONAL_EXCLUSIONS)
    assert any(item["path"] == "fixtures.*" for item in INTENTIONAL_EXCLUSIONS)


def test_production_page_chrome_paths_are_classified_not_drift():
    # Observed live during the #930 canary backfill (2026-07-12): page chrome
    # around the data payloads must classify as raw_only, not schema drift.
    cases = {
        "leaderboard": ["LeagueName"],
        "transfers": [
            # Appeared live 2026-07-17 (acceptance run in the isolated
            # contour): top-level fee-filter bound of the page's slider.
            "maxFee",
        ],
        "match": [
            "seo.eventJSONLD.homeTeam.name",
            "seo.breadcrumbJSONLD[].itemListElement[].item",
            "nav[]",
            "hasPendingVAR",
            "ongoing",
            "content.liveticker.teams[]",
            "content.superlive.showSuperLive",
            "content.table",
            "content.hasPlayoff",
            "content.attackingZones.expected",
            "content.heatmapUrl",
            "content.weather.condition",
            # Appeared mid-canary on 2026-07-13 (44 matches): FotMob started
            # shipping a video-highlights widget inside the match payload.
            "content.highlightStories",
            "content.highlightStories.stories[].provider",
            "content.highlightStories.stories[].content[].restriction.blocked[]",
        ],
        "team": [
            "QAData[].question",
            "allAvailableSeasons[]",
            "seostr",
            "stats.players[].fetchAllUrl",
            "stats.tournamentSeasons[].season",
            "table[].data.annualTable",
            "tabs[]",
            "transfers.type",
        ],
        "player": [
            "context.properties.locale",
            "toggles[].variant.enabled",
            "translations.Advanced",
            "url",
        ],
    }
    for target_type, paths in cases.items():
        coverage = classify_paths(target_type, paths)
        assert not coverage.unknown, (target_type, coverage.unknown)
        assert set(paths) == set(coverage.raw_only), (
            target_type,
            coverage,
        )


def test_entity_map_is_serializable_and_has_all_dispositions():
    mapping = entity_map()
    assert {"all_leagues", "league_season", "leaderboard", "transfers", "match", "team", "player"} <= set(mapping)
    dispositions = {
        rule["disposition"]
        for value in mapping.values()
        for rule in value["rules"]
    }
    assert dispositions == {"typed", "raw_only", "excluded"}
