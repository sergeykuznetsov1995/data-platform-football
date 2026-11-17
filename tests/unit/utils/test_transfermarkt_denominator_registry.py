"""Contract tests for the promoted-registry denominator (#1390)."""

from __future__ import annotations

from dataclasses import replace
import json

import pytest

from dags.utils.transfermarkt_denominator_registry import (
    build_registry_denominator,
    compare_denominators,
    denominator_source,
    diff_status,
)
from scrapers.transfermarkt.denominator import (
    COLUMNS,
    DenominatorError,
    load_denominator,
)
from scrapers.transfermarkt.registry import (
    AgeCategory,
    ClassificationEvidence,
    CompetitionType,
    EvidenceOrigin,
    Gender,
    SeasonFormat,
    TeamType,
)
from tests.unit.dags.test_transfermarkt_scope_planner import (
    _competition,
    _edition,
    _joined_row,
)


def section(
    competition_type,
    label,
    *,
    precedence=2,
    origin=EvidenceOrigin.SOURCE_PAGE,
    **signals,
):
    return ClassificationEvidence(
        source_field="section_label",
        source_value=label,
        source_url="https://example.test/catalogue",
        origin=origin,
        competition_type=competition_type,
        precedence=precedence,
        **signals,
    )


def registry_row(
    competition_id="GB1",
    *,
    competition_type=CompetitionType.DOMESTIC_LEAGUE,
    gender=Gender.MEN,
    team_type=TeamType.CLUB,
    age_category=AgeCategory.SENIOR,
    season_format=SeasonFormat.SPLIT_YEAR,
    edition_id="2026",
    name=None,
    country="England",
    route="wettbewerb",
    evidence=(),
    snapshot="registry-1",
):
    competition = _competition(competition_id, season_format=season_format)
    base = ClassificationEvidence(
        source_field="structured_metadata",
        source_value="test fixture",
        source_url=f"https://example.test/{competition_id}",
        origin=EvidenceOrigin.STRUCTURED,
        competition_type=competition_type,
        gender=gender,
        team_type=team_type,
        age_category=age_category,
        season_format=season_format,
    )
    competition = replace(
        competition,
        name=name or f"Competition {competition_id}",
        country=country,
        competition_type=competition_type,
        gender=gender,
        team_type=team_type,
        age_category=age_category,
        source_url=f"https://example.test/{route}/{competition_id}",
        evidence=(base, *evidence),
        registry_snapshot_id=snapshot,
    )
    edition = replace(
        _edition(competition_id, edition_id, season_format=season_format),
        registry_snapshot_id=snapshot,
    )
    return _joined_row(competition, edition)


def legacy_file(tmp_path, rows):
    path = tmp_path / "denominator.tsv"
    path.write_text(
        "\t".join(COLUMNS)
        + "\n"
        + "".join(
            "\t".join(
                [
                    cid,
                    f"Competition {cid}",
                    "England",
                    "UEFA",
                    tier,
                    klass,
                    route,
                    "1" if live else "0",
                    edition,
                    "",
                    "",
                    "",
                    "",
                    "rule:test",
                ]
            )
            + "\n"
            for cid, klass, tier, route, live, edition in rows
        ),
        encoding="utf-8",
    )
    return path


def test_senior_men_core_and_exclusion_classes():
    rows = [
        registry_row(
            "GB1", evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),)
        ),
        registry_row(
            "FIWC",
            competition_type=CompetitionType.NATIONAL_TEAM_TOURNAMENT,
            team_type=TeamType.NATIONAL_TEAM,
            season_format=SeasonFormat.SINGLE_YEAR,
            edition_id="2026",
            route="pokalwettbewerb",
            evidence=(
                section(
                    CompetitionType.NATIONAL_TEAM_TOURNAMENT,
                    "National Team Competitions",
                ),
            ),
        ),
        registry_row(
            "YOUTH",
            evidence=(
                section(
                    CompetitionType.DOMESTIC_LEAGUE,
                    "First Tier",
                    origin=EvidenceOrigin.NAME,
                    age_category=AgeCategory.UXX,
                ),
            ),
        ),
        registry_row(
            "RESERVE",
            team_type=TeamType.RESERVE,
            evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "Reserve league"),),
        ),
        registry_row(
            "WOMEN",
            gender=Gender.WOMEN,
            evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),),
        ),
    ]
    universe = build_registry_denominator(rows)
    assert universe.universe_version == "registry-1"
    assert universe.denominator.denominator_ids() == {"GB1", "FIWC"}
    classes = {
        entry.row.competition_id: entry.row.competition_class
        for entry in universe.entries
    }
    assert classes == {
        "FIWC": "core_national",
        "GB1": "core_club",
        "RESERVE": "reserve",
        "WOMEN": "excluded_women",
        "YOUTH": "youth",
    }


def test_live_boundaries_and_archive_subtype():
    rows = [
        registry_row(
            "OLD",
            edition_id="2024",
            evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),),
        ),
        registry_row(
            "NEW",
            edition_id="2025",
            evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),),
        ),
        registry_row(
            "NAT21",
            competition_type=CompetitionType.NATIONAL_TEAM_TOURNAMENT,
            team_type=TeamType.NATIONAL_TEAM,
            edition_id="2021",
            evidence=(
                section(
                    CompetitionType.NATIONAL_TEAM_TOURNAMENT,
                    "National Team Competitions",
                ),
            ),
        ),
        registry_row(
            "NAT22",
            competition_type=CompetitionType.NATIONAL_TEAM_TOURNAMENT,
            team_type=TeamType.NATIONAL_TEAM,
            edition_id="2022",
            evidence=(
                section(
                    CompetitionType.NATIONAL_TEAM_TOURNAMENT,
                    "National Team Competitions",
                ),
            ),
        ),
        registry_row(
            "YOUTH",
            age_category=AgeCategory.UXX,
            edition_id="2024",
            evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "Youth league"),),
        ),
        registry_row(
            "RESERVE",
            team_type=TeamType.RESERVE,
            edition_id="2024",
            evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "Reserve league"),),
        ),
    ]
    universe = build_registry_denominator(rows)
    assert universe.denominator.denominator_ids() == {"NEW", "NAT22"}
    entries = {entry.row.competition_id: entry for entry in universe.entries}
    assert entries["OLD"].class_subtype == "core_club"
    assert entries["NAT21"].class_subtype == "core_national"
    assert entries["YOUTH"].class_subtype == "youth"
    assert entries["RESERVE"].class_subtype == "reserve"
    assert all(
        entries[cid].row.competition_class == "archive"
        for cid in ("OLD", "NAT21", "YOUTH", "RESERVE")
    )


def test_unknown_live_is_not_archive(monkeypatch):
    monkeypatch.setattr(
        "dags.utils.transfermarkt_denominator_registry.season_window_year",
        lambda *_args: (_ for _ in ()).throw(ValueError("bad season")),
    )
    row = registry_row(
        "BAD", evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),)
    )
    entry = build_registry_denominator([row]).entries[0]
    assert entry.live_decision is None
    assert entry.row.competition_class == "unknown"
    assert not entry.row.live


def test_tier_precedence_fallback_and_conflicts():
    rows = [
        registry_row(
            "GB1",
            evidence=(
                section(
                    CompetitionType.DOMESTIC_LEAGUE,
                    "National leagues and cups currently not being played",
                    precedence=3,
                ),
                section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),
            ),
        ),
        registry_row(
            "CONFLICT",
            evidence=(
                section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),
                section(CompetitionType.DOMESTIC_LEAGUE, "Second Tier"),
            ),
        ),
        registry_row("NO_TIER"),
        registry_row(
            "MISMATCH",
            evidence=(section(CompetitionType.DOMESTIC_CUP, "Domestic Cup"),),
        ),
        registry_row(
            "KLUB",
            competition_type=CompetitionType.CONTINENTAL_CLUB,
            route="pokalwettbewerb",
        ),
        registry_row(
            "CUP",
            competition_type=CompetitionType.DOMESTIC_CUP,
            route="pokalwettbewerb",
        ),
    ]
    entries = {
        entry.row.competition_id: entry
        for entry in build_registry_denominator(rows).entries
    }
    assert entries["GB1"].row.tier == "1" and entries["GB1"].row.is_core
    assert entries["CONFLICT"].row.competition_class == "unknown"
    assert entries["NO_TIER"].row.competition_class == "unknown"
    assert entries["MISMATCH"].row.competition_class == "conflict"
    assert not entries["MISMATCH"].row.is_core
    assert entries["KLUB"].row.tier == "international" and entries["KLUB"].row.is_core
    assert entries["CUP"].row.tier == "cup" and entries["CUP"].row.is_core


def test_amateur_geography_and_special_policy_ids():
    rows = [
        registry_row(
            "BE5A", evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "Fifth Tier"),)
        ),
        registry_row(
            "BBPO",
            competition_type=CompetitionType.DOMESTIC_CUP,
            name="Landespokal Brandenburg",
            country="Germany",
            route="pokalwettbewerb",
            evidence=(section(CompetitionType.DOMESTIC_CUP, "Further Cup"),),
        ),
        registry_row(
            "NPAM",
            competition_type=CompetitionType.DOMESTIC_CUP,
            name="Regional Cup Lower Saxony (Amateure)",
            country="Germany",
            route="pokalwettbewerb",
            evidence=(section(CompetitionType.DOMESTIC_CUP, "Domestic Cup"),),
        ),
        registry_row(
            "BRRD",
            name="Campeonato Rondoniense",
            country="Americas",
            route="pokalwettbewerb",
            evidence=(
                section(CompetitionType.DOMESTIC_LEAGUE, "Regional Championship"),
            ),
        ),
        registry_row(
            "BRRS",
            name="Campeonato Gaúcho",
            country="Brazil",
            route="pokalwettbewerb",
            evidence=(
                section(CompetitionType.DOMESTIC_LEAGUE, "Regional Championship"),
            ),
        ),
        registry_row(
            "INR1",
            country="India",
            evidence=(
                section(CompetitionType.DOMESTIC_LEAGUE, "Regional Championship"),
            ),
        ),
        registry_row(
            "AG18",
            name="Asian Games",
            competition_type=CompetitionType.DOMESTIC_CUP,
            route="pokalwettbewerb",
            season_format=SeasonFormat.SINGLE_YEAR,
            edition_id="2025",
            evidence=(section(CompetitionType.DOMESTIC_CUP, "International cups"),),
        ),
        registry_row(
            "CACG",
            name="Central American and Caribbean Games",
            competition_type=CompetitionType.DOMESTIC_CUP,
            route="pokalwettbewerb",
            season_format=SeasonFormat.SINGLE_YEAR,
            edition_id="2025",
            evidence=(section(CompetitionType.DOMESTIC_CUP, "International cups"),),
        ),
    ]
    entries = {
        entry.row.competition_id: entry
        for entry in build_registry_denominator(rows).entries
    }
    assert entries["BE5A"].row.competition_class == "amateur"
    assert entries["BBPO"].row.competition_class == "amateur"
    assert entries["NPAM"].row.competition_class == "amateur"
    assert entries["BRRD"].row.competition_class == "unknown"
    assert entries["BRRS"].row.is_core
    assert entries["INR1"].row.competition_class == "amateur"
    assert entries["AG18"].row.competition_class == "youth"
    assert entries["CACG"].row.competition_class == "youth"


def test_title_only_and_quarantine_are_complete_independent_inventories():
    rows = [
        registry_row(
            "GB1", evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),)
        ),
        registry_row(
            "YOUTH",
            age_category=AgeCategory.UXX,
            evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "Youth league"),),
        ),
    ]
    reasons = {
        "D19K": "edition selector must mark exactly one current edition",
        "SU19": "selector SU19",
        "T212": "selector T212",
    }
    universe = build_registry_denominator(
        rows,
        quarantined=reasons,
        title_only_ids=("GB1", "YOUTH", "ABSENT"),
    )
    assert universe.title_only_ids == ("ABSENT", "GB1", "YOUTH")
    assert universe.quarantined == tuple(sorted(reasons.items()))
    assert not set(universe.denominator.rows) & set(reasons)
    assert [entry.title_only for entry in universe.entries] == [True, True]
    with pytest.raises(DenominatorError, match="collision"):
        build_registry_denominator(rows, quarantined={"GB1": "collision"})


def test_validation_dedup_and_current_cardinality():
    row = registry_row(
        "GB1", evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),)
    )
    assert len(build_registry_denominator([row, dict(row)]).entries) == 1
    with pytest.raises(DenominatorError, match="conflicting current"):
        build_registry_denominator([row, {**row, "name": "Conflict"}])
    with pytest.raises(DenominatorError, match="not current"):
        build_registry_denominator([{**row, "is_current": False}])
    with pytest.raises(DenominatorError, match="one exact snapshot"):
        build_registry_denominator([row, registry_row("NEW", snapshot="registry-2")])
    with pytest.raises(DenominatorError, match="empty"):
        build_registry_denominator([])


def test_stored_classification_disagreement_fails_closed():
    row = registry_row(
        "WOMEN",
        gender=Gender.WOMEN,
        evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),),
    )
    row["classification_status"] = "eligible"
    with pytest.raises(DenominatorError):
        build_registry_denominator([row])


def test_dual_run_every_delta_is_explained_and_status_is_recomputable(tmp_path):
    path = legacy_file(
        tmp_path,
        [
            ("GB1", "core_club", "1", "wettbewerb", True, "2025"),
            ("OLD", "core_club", "1", "wettbewerb", True, "2025"),
            ("Q", "core_club", "1", "wettbewerb", True, "2025"),
        ],
    )
    registry = build_registry_denominator(
        [
            registry_row(
                "GB1",
                evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),),
            ),
            registry_row(
                "NEW",
                evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),),
            ),
        ],
        quarantined={"Q": "selector"},
        title_only_ids=("NEW", "ABSENT"),
    )
    report = compare_denominators(
        load_denominator(path), registry, legacy_version="legacy-sha"
    )
    assert report["complete"] is False
    assert report["migration_clean"] is False
    assert report["legacy_core_quarantine_ids"] == ["Q"]
    changes = {row["competition_id"]: row for row in report["denominator_delta"]}
    assert changes["NEW"]["change"] == "added"
    assert "publication_presence" in changes["NEW"]["explanations"]
    assert "title_only" in changes["NEW"]["explanations"]
    assert changes["OLD"]["change"] == "removed"
    assert changes["GB1"]["change"] == "changed"
    assert "current_edition" in changes["GB1"]["explanations"]
    assert changes["Q"]["explanations"] == ["quarantine"]
    assert len(report["title_only"]) == 2
    damaged = dict(report)
    damaged["denominator_delta"] = [dict(row) for row in report["denominator_delta"]]
    damaged["denominator_delta"][0]["explanations"] = []
    status = diff_status(damaged)
    assert status["complete"] is False and status["unexplained"]


def test_source_default_registry_and_rollback(monkeypatch):
    monkeypatch.delenv("TRANSFERMARKT_DENOMINATOR_SOURCE", raising=False)
    assert denominator_source() == "tsv"
    monkeypatch.setenv("TRANSFERMARKT_DENOMINATOR_SOURCE", "registry")
    assert denominator_source() == "registry"
    monkeypatch.setenv("TRANSFERMARKT_DENOMINATOR_SOURCE", "tsv")
    assert denominator_source() == "tsv"
    monkeypatch.setenv("TRANSFERMARKT_DENOMINATOR_SOURCE", "invalid")
    with pytest.raises(DenominatorError, match="unknown denominator source"):
        denominator_source()


def test_real_pinned_registry_matches_independent_oracle():
    from pathlib import Path

    root = Path("/root/data-platform-football/.local/tasks/tm-1390-denominator-offline")
    if not root.is_dir():
        pytest.skip("task-local pinned evidence is not installed")
    rows = json.loads((root / "inputs/promoted-current.json").read_text())
    manifest = json.loads((root / "inputs/discovery-manifest.json").read_text())[
        "manifest"
    ]
    report = manifest["discovery_report"]
    quarantined = {
        cid: report["discovery_quarantined"].get(
            cid, "registry quarantine without detailed discovery reason"
        )
        for cid in manifest["quarantined_competition_ids"]
    }
    universe = build_registry_denominator(
        rows,
        quarantined=quarantined,
        title_only_ids=report["title_only_competition_ids"],
    )
    expected = json.loads((root / "inputs/derived-universe-oracle.json").read_text())
    assert len(universe.entries) == 1824
    assert len(universe.denominator.denominator_ids()) == 944
    assert len(universe.quarantined) == 221
    assert len(universe.title_only_ids) == 30
    fields = {
        "competition_class",
        "team_type",
        "tier",
        "route",
        "live_decision",
        "current_edition",
        "registry_status",
    }
    for entry in universe.entries:
        cid = entry.row.competition_id
        actual = {
            "competition_class": entry.row.competition_class,
            "team_type": entry.team_type,
            "tier": entry.row.tier,
            "route": entry.row.route,
            "live_decision": entry.live_decision,
            "current_edition": str(entry.row.current_saison_id),
            "registry_status": entry.registry_status,
        }
        assert actual == {
            key: value for key, value in expected[cid].items() if key in fields
        }, cid


def test_planner_registry_source_is_lazy_and_materializes_generator(monkeypatch):
    from types import SimpleNamespace
    from dags.utils import transfermarkt_denominator_registry as adapter
    from dags.utils import transfermarkt_scope_planner as planner
    from tests.unit.dags.test_transfermarkt_scope_planner import NOW

    row = registry_row(
        "GB1", evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),)
    )
    selected = build_registry_denominator([row]).denominator
    calls = []

    def fake_build(rows):
        calls.append(list(rows))
        return SimpleNamespace(denominator=selected)

    monkeypatch.setattr(adapter, "build_registry_denominator", fake_build)
    monkeypatch.delenv("TRANSFERMARKT_DENOMINATOR_SOURCE", raising=False)
    planner.plan_transfermarkt_scopes(
        {},
        parent_cycle_id="offline-default",
        registry_rows=[row],
        now=NOW,
        selection_mode="current_only",
    )
    assert calls == []
    monkeypatch.setenv("TRANSFERMARKT_DENOMINATOR_SOURCE", "registry")
    planner.plan_transfermarkt_scopes(
        {},
        parent_cycle_id="offline-registry",
        registry_rows=(item for item in [row]),
        now=NOW,
        selection_mode="current_only",
    )
    assert calls == [[row]]


def test_current_runner_uses_registry_version_without_injection(tmp_path, monkeypatch):
    from dags.scripts import run_transfermarkt_current as current
    from tests.unit.scripts.test_run_transfermarkt_current import Offline

    offline = Offline(tmp_path, monkeypatch)
    monkeypatch.setenv("TRANSFERMARKT_DENOMINATOR_SOURCE", "registry")
    row = registry_row(
        "GB1", evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),)
    )
    report = current.run_current_portion(
        [row],
        {"paid_io_allowed": True, "write_mode": "dual", "revision": 1},
        "registry-source",
        state_dir=offline.root / "registry-state",
        qualification=offline.qualification,
        max_seconds=2400,
        scraper_factory=offline.factory,
        roster_writer=offline.roster_writer,
        career_writer=offline.career_writer,
        coach_writer=offline.coach_writer,
        now_fn=lambda: offline.clock.wall,
        monotonic_fn=lambda: offline.clock.elapsed,
    )
    assert report["denominator_source"] == "registry"
    assert report["universe_version"] == "registry-1"
    assert report["denominator_competition_ids"] == ["GB1"]


def test_offline_cli_dual_run_has_both_versions_and_complete_diff(tmp_path, capsys):
    from dags.scripts import transfermarkt_daily_report as cli

    path = legacy_file(
        tmp_path, [("GB1", "core_club", "1", "wettbewerb", True, "2026")]
    )
    rows = [
        registry_row(
            "GB1", evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),)
        ),
        registry_row(
            "NEW", evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),)
        ),
    ]
    registry_path = tmp_path / "registry.json"
    registry_path.write_text(json.dumps(rows))
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest": {
                    "quarantined_competition_ids": ["D19K"],
                    "discovery_report": {
                        "title_only_competition_ids": ["NEW", "ABSENT"],
                        "discovery_quarantined": {"D19K": "selector"},
                    },
                }
            }
        )
    )
    result = cli.main(
        [
            "--day",
            "2026-11-16",
            "--as-of",
            "2026-11-17T00:00:00+00:00",
            "--registry-json",
            str(registry_path),
            "--denominator",
            str(path),
            "--denominator-dual-run",
            "--discovery-manifest-json",
            str(manifest_path),
        ]
    )
    assert result == 0
    output = json.loads(capsys.readouterr().out)
    assert output["legacy"]["denominator_version"]
    assert output["registry"]["denominator_version"] == "registry-1"
    assert output["diff"]["registry_denominator_count"] == 2
    assert output["diff"]["complete"] is True
    assert output["diff"]["title_only"][0]["present"] is False
    assert output["diff"]["quarantined"] == [
        {"competition_id": "D19K", "reason": "selector"}
    ]


def test_execute_dual_run_uses_one_pinned_registry_and_two_reports(
    tmp_path, monkeypatch
):
    from dags.scripts import transfermarkt_daily_report as cli
    from dags.utils import transfermarkt_native_v2 as native

    path = legacy_file(
        tmp_path, [("GB1", "core_club", "1", "wettbewerb", True, "2026")]
    )
    registry = [
        registry_row(
            "GB1", evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),)
        )
    ]
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "manifest": {
                    "quarantined_competition_ids": [],
                    "discovery_report": {
                        "title_only_competition_ids": [],
                        "discovery_quarantined": {},
                    },
                }
            }
        )
    )
    report = {"measurement_version": "daily-measure-v1"}

    class Cursor:
        description = None

        def __init__(self):
            self.calls = []
            self.closed = False

        def execute(self, sql, params=()):
            self.calls.append((sql, params))
            values = registry if len(self.calls) == 1 else [report]
            self.description = [(key,) for key in values[0]]
            self.rows = [tuple(value.values()) for value in values]

        def fetchall(self):
            return self.rows

        def close(self):
            self.closed = True

    class Connection:
        def __init__(self):
            self.cursor_instance = Cursor()
            self.closed = False

        def cursor(self):
            return self.cursor_instance

        def close(self):
            self.closed = True

    connection = Connection()
    monkeypatch.setattr(native, "connect", lambda: connection)
    result = cli.execute_measure(
        "2026-11-16",
        "2026-11-17T00:00:00+00:00",
        path,
        registry_snapshot_id="registry-1",
        discovery_manifest_path=manifest_path,
        dual_run=True,
    )
    assert len(connection.cursor_instance.calls) == 3
    assert "'registry-1'" in connection.cursor_instance.calls[0][0]
    assert connection.cursor_instance.calls[0][0].endswith(
        "ORDER BY c.competition_id, e.edition_id"
    )
    assert result["legacy"]["report"] == report
    assert result["registry"]["report"] == report
    assert result["registry"]["denominator_version"] == "registry-1"
    assert connection.cursor_instance.closed and connection.closed


def test_daily_dq_routes_default_and_registry(monkeypatch):
    from dags.utils import transfermarkt_daily_dq as daily

    row = registry_row(
        "GB1", evidence=(section(CompetitionType.DOMESTIC_LEAGUE, "First Tier"),)
    )

    class Cursor:
        def __init__(self):
            self.calls = []
            self.description = None

        def execute(self, sql):
            self.calls.append(sql)
            self.description = [(key,) for key in row]

        def fetchall(self):
            return [tuple(row.values())]

    cursor = Cursor()
    monkeypatch.delenv("TRANSFERMARKT_DENOMINATOR_SOURCE", raising=False)
    legacy = daily._routes(cursor)
    assert legacy["GB1"] == "wettbewerb"
    assert cursor.calls == []
    monkeypatch.setenv("TRANSFERMARKT_DENOMINATOR_SOURCE", "registry")
    assert daily._routes(cursor) == {"GB1": "wettbewerb"}
    assert len(cursor.calls) == 1
