from __future__ import annotations

import pytest
import yaml
from pathlib import Path


def test_layout_mode_is_mandatory_and_closed_to_unknown_values() -> None:
    from scrapers.espn.layout import LayoutError, require_layout_mode

    with pytest.raises(LayoutError, match="ESPN_BRONZE_LAYOUT_MODE is required"):
        require_layout_mode(environ={})
    with pytest.raises(LayoutError, match="legacy14 or compact6"):
        require_layout_mode("typo")

    assert require_layout_mode("legacy14") == "legacy14"
    assert require_layout_mode("compact6") == "compact6"


def test_compact6_has_exact_six_public_objects_and_internal_writer_routes() -> None:
    from scrapers.espn.layout import (
        COMPACT6_PUBLIC_OBJECTS,
        relation_location,
    )

    assert COMPACT6_PUBLIC_OBJECTS == {
        "espn_schedule": "VIEW",
        "espn_lineup": "VIEW",
        "espn_matchsheet": "VIEW",
        "espn_ingest_manifest_v2": "BASE TABLE",
        "espn_request_ledger_generation_v2": "BASE TABLE",
        "espn_catalog_snapshot_v2": "BASE TABLE",
    }
    assert relation_location("espn_ingest_manifest_v2", "compact6") == (
        "bronze",
        "espn_ingest_manifest_v2",
    )
    assert relation_location("espn_schedule_generation_v2", "compact6") == (
        "espn_internal",
        "espn_schedule_generation_v2",
    )
    assert relation_location("espn_scope_cutover_v2", "compact6") == (
        "espn_internal",
        "espn_scope_cutover_v2",
    )
    assert relation_location("espn_schedule_current", "compact6") == (
        "espn_internal",
        "espn_schedule_current",
    )


def test_catalog_layout_rejects_public_extras_wrong_kinds_and_mode_drift() -> None:
    from scrapers.espn.layout import (
        COMPACT6_INTERNAL_REQUIRED_OBJECTS,
        COMPACT6_PUBLIC_OBJECTS,
        LayoutError,
        validate_catalog_layout,
    )

    rows = [
        ("bronze", name, kind) for name, kind in COMPACT6_PUBLIC_OBJECTS.items()
    ] + [
        ("espn_internal", name, kind)
        for name, kind in COMPACT6_INTERNAL_REQUIRED_OBJECTS.items()
    ]
    report = validate_catalog_layout("compact6", rows)
    assert report["layout_mode"] == "compact6"
    assert report["public_object_count"] == 6

    with pytest.raises(LayoutError, match="unexpected public ESPN objects"):
        validate_catalog_layout(
            "compact6",
            [*rows, ("bronze", "espn_schedule_current", "VIEW")],
        )
    with pytest.raises(LayoutError, match="unexpected internal ESPN objects"):
        validate_catalog_layout(
            "compact6",
            [*rows, ("espn_internal", "espn_unreviewed_backup", "BASE TABLE")],
        )
    with pytest.raises(LayoutError, match="kind mismatch"):
        validate_catalog_layout(
            "compact6",
            [
                (schema, name, "BASE TABLE" if name == "espn_schedule" else kind)
                for schema, name, kind in rows
            ],
        )
    with pytest.raises(LayoutError, match="catalog does not match legacy14"):
        validate_catalog_layout("legacy14", rows)


def test_legacy14_contract_remains_exact_until_atomic_cutover() -> None:
    from scrapers.espn.layout import LEGACY14_PUBLIC_OBJECTS, relation_location

    assert len(LEGACY14_PUBLIC_OBJECTS) == 14
    assert LEGACY14_PUBLIC_OBJECTS["espn_schedule"] == "BASE TABLE"
    assert LEGACY14_PUBLIC_OBJECTS["espn_schedule_current"] == "VIEW"
    assert relation_location("espn_schedule_generation_v2", "legacy14") == (
        "bronze",
        "espn_schedule_generation_v2",
    )


def test_reviewed_replacements_match_exact_enabled_season_mapping() -> None:
    from scrapers.espn.layout import REVIEWED_NATIVE_REPLACEMENTS

    root = Path(__file__).resolve().parents[3]
    document = yaml.safe_load(
        (root / "configs/espn/season_mapping.yaml").read_text(encoding="utf-8")
    )
    observed = tuple(
        sorted(
            (
                scope_id,
                row["platform_league"],
                row["platform_season_slug"],
            )
            for scope_id, row in document["mappings"].items()
            if row.get("downstream_enabled") is True
        )
    )

    assert observed == REVIEWED_NATIVE_REPLACEMENTS
