"""Strict ESPN source-season to platform-season promotion contracts."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import sys

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[3]
DAGS = ROOT / "dags"
if str(DAGS) not in sys.path:
    sys.path.insert(0, str(DAGS))

MAPPING_PATH = ROOT / "configs" / "espn" / "season_mapping.yaml"
FROZEN_CATALOG_PATH = ROOT / "tests" / "fixtures" / "espn" / "catalog_2026-07-31.json"


def _module():
    from utils import espn_season_mapping

    return espn_season_mapping


def _production_document() -> dict:
    return yaml.safe_load(MAPPING_PATH.read_text(encoding="utf-8"))


def _validate(document: dict, *, scope_agreement: bool = False):
    mod = _module()
    return mod.validate_mapping_document(
        document,
        competitions_document=None,
        registry_document=None,
        require_scope_agreement=scope_agreement,
    )


@pytest.mark.unit
def test_production_mapping_is_exact_six_platform_scopes() -> None:
    catalog = _module().load_season_mapping()

    assert catalog.mapping_version == "2026-08-08"
    assert tuple(item.scope_id for item in catalog.enabled) == (
        "606:2026",
        "700:2026",
        "710:2026",
        "720:2026",
        "730:2026",
        "740:2026",
    )
    assert {(item.platform_league, item.platform_season_slug) for item in catalog.enabled} == {
        ("INT-World Cup", "2026"),
        ("ENG-Premier League", "2627"),
        ("FRA-Ligue 1", "2627"),
        ("GER-Bundesliga", "2627"),
        ("ITA-Serie A", "2627"),
        ("ESP-La Liga", "2627"),
    }


@pytest.mark.unit
def test_exact_six_enabled_and_175_frozen_scopes_excluded() -> None:
    catalog = _module().load_season_mapping()
    frozen = json.loads(FROZEN_CATALOG_PATH.read_text(encoding="utf-8"))
    male_scopes = {
        f"{row['espn_id']}:{row['source_season_year']}"
        for row in frozen["candidates"]
        if row["gender"] == "MALE"
    }
    enabled = {item.scope_id for item in catalog.enabled}

    assert len(male_scopes) == 181
    assert enabled <= male_scopes
    assert len(enabled) == 6
    assert len(male_scopes - enabled) == 175


@pytest.mark.unit
def test_explicit_conventions_do_not_apply_a_global_year_formula() -> None:
    document = _production_document()
    document["mapping_version"] = "2042-01-01"
    document["mappings"]["999:2042"] = {
        "espn_id": 999,
        "source_season_year": 2042,
        "platform_league": "BRA-Serie A",
        "platform_season_slug": "2042",
        "convention": "calendar_year",
        "effective_start_date": "2042-01-01",
        "effective_end_date": "2042-12-31",
        "approval": {
            "approved_by": "test-owner",
            "approved_at": "2041-12-01",
            "reference": "test://calendar-year",
        },
    }

    catalog = _validate(document)
    synthetic = catalog.by_scope["999:2042"]

    assert synthetic.downstream_enabled is False
    assert synthetic.platform_season_slug == "2042"
    assert synthetic.convention == "calendar_year"
    assert catalog.by_scope["700:2026"].platform_season_slug == "2627"
    assert catalog.by_scope["606:2026"].platform_season_slug == "2026"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda d: d["mappings"]["700:2026"].update(source_season_year=2025),
            "scope suffix",
        ),
        (
            lambda d: d["mappings"]["700:2026"].update(convention="guess"),
            "convention",
        ),
        (
            lambda d: d["mappings"]["700:2026"].pop("approval"),
            "approval",
        ),
        (
            lambda d: d["mappings"]["700:2026"].update(
                effective_start_date="2027-06-01"
            ),
            "date window",
        ),
        (
            lambda d: d["mappings"]["710:2026"].update(
                platform_league="ENG-Premier League"
            ),
            "platform pair",
        ),
    ],
)
def test_bad_mapping_fails_closed(mutate, message) -> None:
    document = _production_document()
    mutate(document)

    with pytest.raises(ValueError, match=message):
        _validate(document)


@pytest.mark.unit
def test_overlapping_enabled_windows_for_one_platform_league_fail_closed() -> None:
    document = _production_document()
    duplicate = deepcopy(document["mappings"]["700:2026"])
    duplicate.update(
        espn_id=701,
        source_season_year=2027,
        platform_season_slug="2728",
        effective_start_date="2027-05-01",
        effective_end_date="2028-05-31",
    )
    document["mappings"]["701:2027"] = duplicate

    with pytest.raises(ValueError, match="overlap"):
        _validate(document)


@pytest.mark.unit
def test_duplicate_yaml_mapping_key_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "mapping.yaml"
    path.write_text(
        "schema_version: 1\n"
        "mapping_version: 'test'\n"
        "mappings:\n"
        "  '700:2026': {}\n"
        "  '700:2026': {}\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate YAML key"):
        _module().load_season_mapping(
            path=path,
            competitions_path=None,
            registry_path=None,
            require_scope_agreement=False,
        )


@pytest.mark.unit
def test_missing_enabled_mapping_fails_exact_medallion_scope_agreement() -> None:
    document = _production_document()
    del document["mappings"]["740:2026"]

    with pytest.raises(ValueError, match="in_scope"):
        _module().validate_mapping_document(
            document,
            competitions_document=yaml.safe_load(
                (ROOT / "configs" / "medallion" / "competitions.yaml").read_text(
                    encoding="utf-8"
                )
            ),
            registry_document=yaml.safe_load(
                (ROOT / "configs" / "espn" / "registry.yaml").read_text(
                    encoding="utf-8"
                )
            ),
            require_scope_agreement=True,
        )


@pytest.mark.unit
def test_old_platform_season_fails_exact_latest_scope_agreement() -> None:
    document = _production_document()
    document["mappings"]["700:2026"].update(
        platform_season_slug="2526",
        effective_start_date="2025-08-15",
        effective_end_date="2026-05-24",
    )

    with pytest.raises(ValueError, match="latest in_scope"):
        _module().validate_mapping_document(
            document,
            competitions_document=yaml.safe_load(
                (ROOT / "configs" / "medallion" / "competitions.yaml").read_text(
                    encoding="utf-8"
                )
            ),
            registry_document=yaml.safe_load(
                (ROOT / "configs" / "espn" / "registry.yaml").read_text(
                    encoding="utf-8"
                )
            ),
            require_scope_agreement=True,
        )


@pytest.mark.unit
def test_approval_cannot_postdate_mapping_version() -> None:
    document = _production_document()
    document["mappings"]["700:2026"]["approval"]["approved_at"] = "2026-08-09"

    with pytest.raises(ValueError, match="approval date"):
        _validate(document)


@pytest.mark.unit
def test_platform_slug_must_be_approved_registry_legacy_alias() -> None:
    document = _production_document()
    competitions = yaml.safe_load(
        (ROOT / "configs" / "medallion" / "competitions.yaml").read_text(
            encoding="utf-8"
        )
    )
    registry = yaml.safe_load(
        (ROOT / "configs" / "espn" / "registry.yaml").read_text(
            encoding="utf-8"
        )
    )
    epl = next(row for row in registry["competitions"] if row["espn_id"] == 700)
    epl["legacy"]["season_aliases"][2026] = ["2026"]

    with pytest.raises(ValueError, match="legacy season_aliases"):
        _module().validate_mapping_document(
            document,
            competitions_document=competitions,
            registry_document=registry,
            require_scope_agreement=True,
        )


@pytest.mark.unit
def test_renderer_emits_exact_allowlist_and_common_fail_closed_filter() -> None:
    mod = _module()
    template = """WITH espn_downstream_scope (
scope_id, espn_id, source_season_year, platform_league,
platform_season_slug, convention, effective_start_date, effective_end_date
) AS (VALUES
__ESPN_DOWNSTREAM_SCOPE_VALUES__
)
SELECT * FROM source es_source
JOIN espn_downstream_scope espn_scope ON
__ESPN_DOWNSTREAM_SCOPE_FILTER__
"""

    rendered = mod.render_espn_downstream_sql(template)

    assert "__ESPN_DOWNSTREAM" not in rendered
    assert rendered.count("('606:2026'") == 1
    assert rendered.count("('700:2026'") == 1
    assert "es_source.scope_id = espn_scope.scope_id" in rendered
    assert "es_source.scope_id IS NULL" not in rendered
    assert "es_source.league = espn_scope.platform_league" not in rendered
    assert "effective_start_date" in rendered
    assert "effective_end_date" in rendered
    assert "effective_start_date - INTERVAL '1' DAY" in rendered
    assert "effective_end_date + INTERVAL '1' DAY" in rendered


@pytest.mark.unit
def test_renderer_rejects_half_wired_marker_pair() -> None:
    with pytest.raises(ValueError, match="both ESPN downstream markers"):
        _module().render_espn_downstream_sql(
            "SELECT __ESPN_DOWNSTREAM_SCOPE_VALUES__"
        )


@pytest.mark.unit
def test_renderer_rejects_typoed_espn_marker_before_trino() -> None:
    with pytest.raises(ValueError, match="unknown ESPN downstream SQL markers"):
        _module().render_espn_downstream_sql(
            "SELECT __ESPN_DOWNSTREAM_SCOPE_VALUSE__"
        )
