from scrapers.fotmob.field_map import (
    INTENTIONAL_EXCLUSIONS,
    FieldDisposition,
    FieldRule,
    classify_paths,
    entity_map,
)


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


def test_intentional_exclusions_are_exported_with_reasons():
    assert INTENTIONAL_EXCLUSIONS
    assert all(item["reason"] for item in INTENTIONAL_EXCLUSIONS)
    assert any(item["path"] == "content.buzz.*" for item in INTENTIONAL_EXCLUSIONS)
    assert any(item["path"] == "fixtures.*" for item in INTENTIONAL_EXCLUSIONS)


def test_entity_map_is_serializable_and_has_all_dispositions():
    mapping = entity_map()
    assert {"all_leagues", "league_season", "leaderboard", "transfers", "match", "team", "player"} <= set(mapping)
    dispositions = {
        rule["disposition"]
        for value in mapping.values()
        for rule in value["rules"]
    }
    assert dispositions == {"typed", "raw_only", "excluded"}
