from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from scrapers.transfermarkt.registry import (
    BOOTSTRAP_COMPETITIONS,
    AgeCategory,
    ClassificationEvidence,
    ClassificationStatus,
    CompetitionType,
    CrawlScope,
    EvidenceOrigin,
    Gender,
    IncompleteSnapshotError,
    RegistryConflictError,
    RegistryPage,
    SeasonFormat,
    TeamType,
    UnknownCompetitionError,
    UnsafeCrawlError,
    canonical_season,
    deterministic_scope_id,
    reconcile_registry_pages,
    resolve_competition,
)


FIXTURE = (
    Path(__file__).parents[2]
    / "fixtures"
    / "transfermarkt"
    / "registry"
    / "registry_snapshot.yaml"
)


@pytest.fixture(scope="module")
def raw_registry() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def pages(raw_registry: dict) -> tuple[RegistryPage, ...]:
    return tuple(RegistryPage.from_mapping(item) for item in raw_registry["pages"])


@pytest.fixture(scope="module")
def snapshot(raw_registry: dict, pages: tuple[RegistryPage, ...]):
    return reconcile_registry_pages(
        reversed(pages),
        expected_page_count=2,
        expected_competition_ids=raw_registry["expected_competition_ids"],
    )


@pytest.mark.parametrize(
    ("value", "season_format", "expected"),
    [
        ("2025/26", SeasonFormat.SPLIT_YEAR, "2526"),
        ("2025-2026", "split_year", "2526"),
        (2025, SeasonFormat.SPLIT_YEAR, "2526"),
        ("1999/00", SeasonFormat.SPLIT_YEAR, "9900"),
        (2026, SeasonFormat.SINGLE_YEAR, "2026"),
    ],
)
def test_canonical_season_requires_explicit_format(
    value, season_format, expected
) -> None:
    assert canonical_season(value, season_format) == expected


def test_single_year_is_never_unconditionally_changed_to_split_year() -> None:
    assert canonical_season(2026, SeasonFormat.SINGLE_YEAR) == "2026"
    assert canonical_season(2026, SeasonFormat.SPLIT_YEAR) == "2627"
    with pytest.raises(ValueError, match="unknown"):
        canonical_season(2026, SeasonFormat.UNKNOWN)
    with pytest.raises(ValueError, match="single-year"):
        canonical_season("2025/26", SeasonFormat.SINGLE_YEAR)
    with pytest.raises(ValueError, match="span one year"):
        canonical_season("2025/27", SeasonFormat.SPLIT_YEAR)


def test_only_verified_bootstrap_records_are_present_and_resolvable() -> None:
    assert {item.competition_id for item in BOOTSTRAP_COMPETITIONS} == {
        "GB1",
        "ES1",
        "IT1",
        "L1",
        "FR1",
        "CL",
        "AFCN",
        "UNLA",
        "FIWC",
    }
    assert all(item.crawl_eligible for item in BOOTSTRAP_COMPETITIONS)
    assert resolve_competition("ENG-Premier League").competition_id == "GB1"
    assert resolve_competition("English Premier League").competition_id == "GB1"
    assert resolve_competition("uefa-champions-league").competition_id == "CL"
    assert resolve_competition("INT-World Cup").competition_id == "FIWC"
    with pytest.raises(UnknownCompetitionError):
        resolve_competition("NOPE-Not A League")


# #948: the seed is the only bridge between a Transfermarkt competition_id and
# the legacy canonical id.  Discovery stamps canonical_competition_id from
# resolve_competition(competition_id), and the reader cutover gate demands that
# every legacy (league, season) pair exists in the v2 slot — so a Top-5 league
# whose canonical id is absent (or differs by one character) from
# configs/medallion/competitions.yaml can never satisfy the gate.
TOP5_TM_IDS = ("GB1", "ES1", "IT1", "L1", "FR1")

LEGACY_COMPETITIONS = (
    Path(__file__).parents[3] / "configs" / "medallion" / "competitions.yaml"
)


@pytest.fixture(scope="module")
def legacy_competition_ids() -> set[str]:
    doc = yaml.safe_load(LEGACY_COMPETITIONS.read_text(encoding="utf-8"))
    return {item["id"] for item in doc["competitions"]}


def test_top5_canonical_ids_match_the_legacy_medallion_contour(
    legacy_competition_ids: set[str],
) -> None:
    """Canonical ids must match competitions.yaml character for character.

    Read from the YAML on purpose: a literal copy here would let the two sides
    drift apart silently, which is exactly the failure the cutover gate cannot
    survive.
    """

    canonical_by_tm_id = {
        record.competition_id: record.canonical_competition_id
        for record in BOOTSTRAP_COMPETITIONS
    }
    seeded_top5 = {tm_id: canonical_by_tm_id[tm_id] for tm_id in TOP5_TM_IDS}

    assert all(seeded_top5.values()), f"Top-5 canonical id is unset: {seeded_top5}"
    unknown = set(seeded_top5.values()) - legacy_competition_ids
    assert not unknown, f"canonical ids absent from competitions.yaml: {sorted(unknown)}"

    # And the seed covers every Top-5 league the legacy contour knows about.
    top5_legacy = {
        "ENG-Premier League",
        "ESP-La Liga",
        "ITA-Serie A",
        "GER-Bundesliga",
        "FRA-Ligue 1",
    }
    assert top5_legacy <= legacy_competition_ids  # guard: yaml still spells them so
    assert set(seeded_top5.values()) == top5_legacy


@pytest.mark.parametrize(
    ("tm_id", "canonical", "slug", "alias"),
    [
        ("ES1", "ESP-La Liga", "laliga", "Spanish La Liga"),
        ("IT1", "ITA-Serie A", "serie-a", "Italian Serie A"),
        ("L1", "GER-Bundesliga", "bundesliga", "German Bundesliga"),
        ("FR1", "FRA-Ligue 1", "ligue-1", "French Ligue 1"),
    ],
)
def test_top5_resolves_from_source_and_canonical_identities(
    tm_id: str, canonical: str, slug: str, alias: str
) -> None:
    resolved = {
        identity: resolve_competition(identity)
        for identity in (tm_id, f"TM-{tm_id}", canonical, slug, alias)
    }
    assert {record.competition_id for record in resolved.values()} == {tm_id}
    assert {record.canonical_competition_id for record in resolved.values()} == {
        canonical
    }


def test_bootstrap_identities_are_unambiguous() -> None:
    """No seeded identity may resolve to two records (UnknownCompetitionError:
    ambiguous) — adding the Top-5 must not collide with the existing records."""

    for record in BOOTSTRAP_COMPETITIONS:
        identities = (
            record.competition_id,
            f"TM-{record.competition_id}",
            record.slug,
            record.name,
            record.canonical_competition_id,
            *record.aliases,
        )
        for identity in filter(None, identities):
            assert (
                resolve_competition(identity).competition_id == record.competition_id
            ), f"{identity!r} did not resolve back to {record.competition_id}"


def test_top5_season_format_is_split_year() -> None:
    """Pin protecting the Silver market-value transform.

    ``dags/sql/silver/transfermarkt_market_value_history.sql`` selects the
    season formula from the published registry: ``single_year`` -> calendar
    year, anything else -> the split-year slug.  Top-5 leagues are split-year
    at the source; flipping this seed would silently restate their seasons.
    """

    for tm_id in TOP5_TM_IDS:
        record = resolve_competition(tm_id)
        assert record.season_format is SeasonFormat.SPLIT_YEAR, tm_id
        assert record.competition_type is CompetitionType.DOMESTIC_LEAGUE, tm_id
        assert record.team_type is TeamType.CLUB, tm_id


def test_runtime_resolver_uses_discovered_records_instead_of_bootstrap(
    snapshot,
) -> None:
    resolved = resolve_competition("GB1", snapshot.competitions)
    assert resolved.registry_snapshot_id == "fixture-2026-07-11"
    with pytest.raises(UnknownCompetitionError):
        resolve_competition("FIWC", records=[])


def test_reconcile_requires_every_declared_page(
    pages: tuple[RegistryPage, ...],
) -> None:
    with pytest.raises(IncompleteSnapshotError, match=r"missing=\[2\]"):
        reconcile_registry_pages(pages[:1])
    with pytest.raises(IncompleteSnapshotError, match="page_count mismatch"):
        reconcile_registry_pages(pages, expected_page_count=3)


def test_reconcile_is_deterministic_and_checks_inventory(
    pages: tuple[RegistryPage, ...], snapshot
) -> None:
    forward = reconcile_registry_pages(pages)
    assert forward.snapshot_hash == snapshot.snapshot_hash
    assert forward.competitions == snapshot.competitions
    assert forward.editions == snapshot.editions
    with pytest.raises(IncompleteSnapshotError, match="inventory mismatch"):
        reconcile_registry_pages(pages, expected_competition_ids={"GB1"})


def test_conflicting_duplicate_source_identity_blocks_snapshot(
    pages: tuple[RegistryPage, ...],
) -> None:
    gb1 = next(item for item in pages[0].competitions if item.competition_id == "GB1")
    conflicting = replace(gb1, name="Different Competition")
    page_two = replace(
        pages[1], competitions=pages[1].competitions + (conflicting,)
    )
    with pytest.raises(RegistryConflictError, match="conflicting competition"):
        reconcile_registry_pages((pages[0], page_two))


def test_all_required_positive_fixtures_have_safe_scopes(snapshot) -> None:
    assert snapshot.blocked_competition_ids == ("MYSTERY",)
    assert snapshot.promotable is False
    with pytest.raises(UnsafeCrawlError, match="MYSTERY"):
        snapshot.crawl_scopes()

    scopes = snapshot.crawl_scopes(strict=False)
    assert {(item.competition_id, item.canonical_season) for item in scopes} == {
        ("GB1", "2526"),
        ("CL", "2526"),
        ("AFCN", "2025"),
        ("UNLA", "2627"),
        ("FIWC", "2026"),
    }
    assert len({item.scope_id for item in scopes}) == len(scopes)


def test_women_youth_and_reserve_are_source_proven_exclusions(snapshot) -> None:
    records = {item.competition_id: item for item in snapshot.competitions}
    assert records["GB1W"].classification_status is ClassificationStatus.EXCLUDED
    assert records["U21EC"].classification_status is ClassificationStatus.EXCLUDED
    assert records["GB1R"].classification_status is ClassificationStatus.EXCLUDED
    assert "gender=women" in records["GB1W"].crawl_block_reason
    assert "age_category=uxx" in records["U21EC"].crawl_block_reason
    assert "team_type=reserve" in records["GB1R"].crawl_block_reason


def test_name_is_not_sufficient_positive_classification_evidence(snapshot) -> None:
    mystery = next(
        item for item in snapshot.competitions if item.competition_id == "MYSTERY"
    )
    assert mystery.name == "Men's Senior Mystery League"
    assert mystery.classification_status is ClassificationStatus.UNKNOWN
    assert "missing non-name source evidence" in mystery.crawl_block_reason


def test_conflicting_source_evidence_blocks_crawl(snapshot) -> None:
    gb1 = next(item for item in snapshot.competitions if item.competition_id == "GB1")
    conflict = ClassificationEvidence(
        source_field="audience_marker",
        source_value="women",
        source_url=gb1.source_url,
        origin=EvidenceOrigin.STRUCTURED,
        gender=Gender.WOMEN,
    )
    unsafe = replace(gb1, evidence=gb1.evidence + (conflict,))
    edition = next(
        item for item in snapshot.editions if item.competition_id == "GB1"
    )
    assert unsafe.classification_status is ClassificationStatus.CONFLICT
    with pytest.raises(UnsafeCrawlError, match="conflicting source evidence"):
        CrawlScope.from_records(unsafe, edition)


def test_domestic_cup_is_supported_with_structured_evidence(snapshot) -> None:
    gb1 = next(item for item in snapshot.competitions if item.competition_id == "GB1")
    context = ClassificationEvidence(
        source_field="competition_context",
        source_value="domestic_cup:club",
        source_url="https://www.transfermarkt.com/fa-cup/startseite/pokalwettbewerb/FAC",
        origin=EvidenceOrigin.SOURCE_PAGE,
        competition_type=CompetitionType.DOMESTIC_CUP,
        team_type=TeamType.CLUB,
    )
    cup = replace(
        gb1,
        competition_id="FAC",
        slug="fa-cup",
        name="FA Cup",
        competition_type=CompetitionType.DOMESTIC_CUP,
        source_url=context.source_url,
        evidence=(context, *gb1.evidence[1:]),
    )
    assert cup.classification_status is ClassificationStatus.ELIGIBLE
    assert cup.crawl_eligible is True


def test_competition_and_edition_bronze_contract_columns_are_exact(snapshot) -> None:
    competition = next(
        item for item in snapshot.competitions if item.competition_id == "GB1"
    )
    edition = next(
        item for item in snapshot.editions if item.competition_id == "GB1"
    )
    assert tuple(competition.as_dict()) == (
        "competition_id",
        "slug",
        "name",
        "country",
        "confederation",
        "competition_type",
        "gender",
        "team_type",
        "age_category",
        "season_format",
        "active",
        "source_url",
        "discovered_at",
        "canonical_competition_id",
        "classification_status",
        "classification_evidence",
        "registry_snapshot_id",
        "source_body_hash",
        "parser_revision",
        "schema_revision",
    )
    assert tuple(edition.as_dict()) == (
        "competition_id",
        "edition_id",
        "edition_label",
        "canonical_season",
        "season_format",
        "start_date",
        "end_date",
        "active",
        "current",
        "participant_count",
        "participant_hash",
        "source_url",
        "discovered_at",
        "registry_snapshot_id",
        "source_body_hash",
        "parser_revision",
        "schema_revision",
    )
    assert json.loads(competition.as_dict()["classification_evidence"])


def test_scope_identity_is_stable_and_sensitive_to_exact_source_identity(
    snapshot,
) -> None:
    first = deterministic_scope_id("GB1", "2025")
    assert first == deterministic_scope_id("GB1", "2025")
    assert first.startswith("tm-") and len(first) == 27
    assert first != deterministic_scope_id("GB1", "2024")
    assert first != deterministic_scope_id("gb1", "2025")

    scope = next(
        item for item in snapshot.crawl_scopes(strict=False) if item.competition_id == "GB1"
    )
    assert scope.scope_id == first
    assert scope.registry_snapshot_id == snapshot.snapshot_id


def test_competition_team_type_conflict_is_fail_closed(snapshot) -> None:
    gb1 = next(item for item in snapshot.competitions if item.competition_id == "GB1")
    national_signal = ClassificationEvidence(
        source_field="team_type",
        source_value="national_team",
        source_url=gb1.source_url,
        origin=EvidenceOrigin.STRUCTURED,
        team_type=TeamType.NATIONAL_TEAM,
    )
    conflict = replace(
        gb1,
        team_type=TeamType.NATIONAL_TEAM,
        evidence=gb1.evidence + (national_signal,),
    )
    assert conflict.classification_status is ClassificationStatus.CONFLICT


def test_fixture_has_explicit_senior_mens_dimensions(snapshot) -> None:
    positives = {
        item.competition_id: item
        for item in snapshot.competitions
        if item.classification_status is ClassificationStatus.ELIGIBLE
    }
    assert set(positives) == {"GB1", "CL", "AFCN", "UNLA", "FIWC"}
    assert all(item.gender is Gender.MEN for item in positives.values())
    assert all(item.age_category is AgeCategory.SENIOR for item in positives.values())
