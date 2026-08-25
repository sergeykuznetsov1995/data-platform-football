from __future__ import annotations

import importlib.util
import sys
import uuid
from pathlib import Path

import pytest

from scrapers.fbref.raw_store import RawPageStore, competition_page_target


SCRIPT = (
    Path(__file__).parents[3]
    / "scripts"
    / "research"
    / "remediate_fbref_current_seasons.py"
)
SPEC = importlib.util.spec_from_file_location(
    "_remediate_fbref_current_seasons", SCRIPT
)
assert SPEC is not None and SPEC.loader is not None
remediation = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = remediation
SPEC.loader.exec_module(remediation)


class FakeControl:
    def __init__(self, competitions, currents):
        self.competitions = list(competitions)
        self.currents = list(currents)

    def eligible_competitions(self):
        return list(self.competitions)

    def list_seasons(self, *, current, limit, after=None):
        assert current is True
        rows = sorted(
            self.currents,
            key=lambda row: (row["competition_id"], row["season_id"]),
        )
        if after is not None:
            rows = [
                row for row in rows if (row["competition_id"], row["season_id"]) > after
            ]
        return rows[:limit]


def _competition(
    competition_id,
    *,
    last_season,
    last_season_url,
    classification="other:national_team",
):
    return {
        "source": "fbref",
        "competition_id": competition_id,
        "canonical_url": (f"https://fbref.com/en/comps/{competition_id}/history/x"),
        "name": f"Competition {competition_id}",
        "gender": "male",
        "classification": classification,
        "metadata": {
            "last_season": last_season,
            "last_season_url": last_season_url,
        },
        "last_snapshot_id": f"snapshot-{competition_id}",
    }


def _current(competition_id, season_id, url):
    return {
        "competition_id": competition_id,
        "season_id": season_id,
        "canonical_url": url,
        "label": season_id,
        "is_current": True,
    }


def _commit_history(raw, competition, html):
    target = competition_page_target(
        competition["competition_id"], competition["canonical_url"]
    )
    return raw.commit_fetch(
        target,
        html.encode(),
        logical_refresh_id=str(uuid.uuid4()),
        attempt_id=str(uuid.uuid4()),
        http_status=200,
    )


def test_dry_run_proposes_exact_comp6_source_season_without_writes(tmp_path):
    raw = RawPageStore.from_uri(tmp_path.as_uri())
    competition = _competition(
        "6",
        last_season="2026",
        last_season_url="https://fbref.com/en/comps/6/WCQ----UEFA-M-Stats",
    )
    _commit_history(
        raw,
        competition,
        """
      <table id="seasons"><tbody><tr><th data-stat="season"><a
        href="/en/comps/6/2022/2022-WCQ----UEFA-M-Stats"
      >2022 FIFA World Cup Qualification — UEFA</a></th></tr></tbody></table>
    """,
    )
    control = FakeControl(
        [competition],
        [
            _current(
                "6",
                "2022",
                "https://fbref.com/en/comps/6/2022/2022-WCQ----UEFA-M-Stats",
            )
        ],
    )
    writes = []

    evidence = remediation.run_remediation(
        control,
        raw,
        competition_ids=["6"],
        apply=False,
        apply_one=lambda *_args: writes.append(_args),
    )

    assert writes == []
    assert evidence["mode"] == "dry_run"
    assert evidence["plans"] == [
        {
            "competition_id": "6",
            "installed_current_season_id": "2022",
            "advertised_current_season_id": "2026",
            "advertised_current_label": "2026",
            "advertised_current_url": (
                "https://fbref.com/en/comps/6/WCQ----UEFA-M-Stats"
            ),
            "action": "reconcile",
            "history_content_hash": evidence["plans"][0]["history_content_hash"],
            "history_logical_refresh_id": evidence["plans"][0][
                "history_logical_refresh_id"
            ],
            "competition_snapshot_id": "snapshot-6",
            "install_contract_version": ("fbref-current-season-install-source-link-v1"),
        }
    ]

    applied = []
    apply_evidence = remediation.run_remediation(
        control,
        raw,
        competition_ids=["6"],
        apply=True,
        source_run_id=str(uuid.uuid4()),
        apply_one=lambda *args: applied.append(args),
    )
    assert apply_evidence["mode"] == "apply"
    assert len(applied) == 1
    assert applied[0][2].advertised_current_season_id == "2026"


def test_comp255_same_source_id_is_no_change_not_demotion(tmp_path):
    raw = RawPageStore.from_uri(tmp_path.as_uri())
    current_url = (
        "https://fbref.com/en/comps/255/2026/2026-FIFA-World-Cup-Qualification-Stats"
    )
    competition = _competition(
        "255",
        last_season="2026",
        last_season_url=current_url,
        classification="cup:national_team",
    )
    _commit_history(
        raw,
        competition,
        """
      <html><body><main><div class="content_grid"><a
        href="/en/comps/255/2026/2026-FIFA-World-Cup-Qualification-Stats"
      >2026 FIFA World Cup Qualification — Inter-confederation play-offs</a>
      </div></main></body></html>
    """,
    )
    control = FakeControl(
        [competition],
        [_current("255", "2026", current_url)],
    )

    evidence = remediation.run_remediation(
        control,
        raw,
        competition_ids=["255"],
        apply=False,
    )

    assert evidence["plans"][0]["action"] == "no_change"
    assert evidence["plans"][0]["advertised_current_season_id"] == "2026"


def test_apply_requires_source_run_id_before_any_write(tmp_path):
    with pytest.raises(
        remediation.RemediationConfigurationError,
        match="source_run_id is required",
    ):
        remediation.run_remediation(
            FakeControl([], []),
            RawPageStore.from_uri(tmp_path.as_uri()),
            competition_ids=["6"],
            apply=True,
        )


def test_apply_refuses_missing_source_run_raw_lineage(tmp_path):
    raw = RawPageStore.from_uri(tmp_path.as_uri())
    competition = _competition(
        "6",
        last_season="2026",
        last_season_url="https://fbref.com/en/comps/6/WCQ----UEFA-M-Stats",
    )
    _commit_history(
        raw,
        competition,
        """
      <table id="seasons"><tbody><tr><th data-stat="season"><a
        href="/en/comps/6/2022/2022-WCQ----UEFA-M-Stats"
      >2022 FIFA World Cup Qualification — UEFA</a></th></tr></tbody></table>
    """,
    )

    class MissingLineageControl(FakeControl):
        def list_fetch_attempts_for_refresh(self, run_id, logical_refresh_id):
            return []

    control = MissingLineageControl(
        [competition],
        [
            _current(
                "6",
                "2022",
                "https://fbref.com/en/comps/6/2022/2022-WCQ----UEFA-M-Stats",
            )
        ],
    )
    plan = remediation.build_remediation_plans(control, raw, competition_ids=["6"])[0]

    with pytest.raises(
        remediation.RemediationEvidenceError,
        match="source-run raw lineage is not singular",
    ):
        remediation._apply_one(
            control,
            raw,
            plan,
            str(uuid.uuid4()),
        )


def test_requested_competition_requires_exactly_one_current_season(tmp_path):
    raw = RawPageStore.from_uri(tmp_path.as_uri())
    competition = _competition(
        "6",
        last_season="2026",
        last_season_url="https://fbref.com/en/comps/6/WCQ----UEFA-M-Stats",
    )

    with pytest.raises(
        remediation.RemediationEvidenceError,
        match="has no installed current season",
    ):
        remediation.run_remediation(
            FakeControl([competition], []),
            raw,
            competition_ids=["6"],
            apply=False,
        )


def test_apply_preflights_complete_batch_before_first_write(tmp_path, monkeypatch):
    raw = RawPageStore.from_uri(tmp_path.as_uri())
    competitions = []
    currents = []
    for competition_id in ("6", "678"):
        competition = _competition(
            competition_id,
            last_season="2026",
            last_season_url=(
                f"https://fbref.com/en/comps/{competition_id}/Current-Stats"
            ),
        )
        competitions.append(competition)
        currents.append(
            _current(
                competition_id,
                "2022",
                f"https://fbref.com/en/comps/{competition_id}/2022/Old-Stats",
            )
        )
        _commit_history(
            raw,
            competition,
            f"""
          <table id="seasons"><tbody><tr><th data-stat="season"><a
            href="/en/comps/{competition_id}/2022/Old-Stats"
          >2022 Old season</a></th></tr></tbody></table>
        """,
        )

    class SecondLineageMissingControl(FakeControl):
        def __init__(self):
            super().__init__(competitions, currents)
            self.calls = 0

        def list_fetch_attempts_for_refresh(self, run_id, logical_refresh_id):
            self.calls += 1
            if self.calls == 2:
                return []
            plan = plans[0]
            return [
                {
                    "status": "succeeded",
                    "target_id": plan._history_record.target_id,
                    "content_hash": plan.history_content_hash,
                    "raw_manifest_key": "manifest.json",
                }
            ]

    control = SecondLineageMissingControl()
    plans = remediation.build_remediation_plans(
        control,
        raw,
        competition_ids=["6", "678"],
    )
    writes = []
    monkeypatch.setattr(remediation, "_install_one", lambda *_args: writes.append(1))

    with pytest.raises(
        remediation.RemediationEvidenceError,
        match="source-run raw lineage is not singular",
    ):
        remediation.run_remediation(
            control,
            raw,
            competition_ids=["6", "678"],
            apply=True,
            source_run_id=str(uuid.uuid4()),
        )

    assert writes == []


@pytest.mark.parametrize("competition_ids", [[], ["6", "6"]])
def test_scope_must_be_explicit_and_unique(tmp_path, competition_ids):
    with pytest.raises(remediation.RemediationConfigurationError):
        remediation.run_remediation(
            FakeControl([], []),
            RawPageStore.from_uri(tmp_path.as_uri()),
            competition_ids=competition_ids,
            apply=False,
        )
