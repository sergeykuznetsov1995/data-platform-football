from __future__ import annotations

import importlib.util
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from scrapers.fbref.raw_store import (
    RawPageStore,
    competition_index_target,
    competition_page_target,
)


SCRIPT = (
    Path(__file__).parents[3]
    / "scripts"
    / "research"
    / "remediate_fbref_current_seasons.py"
)
RUNBOOK = (
    Path(__file__).parents[3]
    / "docs"
    / "operations"
    / "fbref_current_season_reconcile.md"
)
SPEC = importlib.util.spec_from_file_location(
    "_remediate_fbref_current_seasons", SCRIPT
)
assert SPEC is not None and SPEC.loader is not None
remediation = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = remediation
SPEC.loader.exec_module(remediation)


HISTORY_RUN_ID = str(uuid.UUID(int=901))


def test_runbook_uses_only_readme_immutable_blob_function_for_apply():
    runbook = RUNBOOK.read_text()

    assert '< "$remediation_source"' not in runbook
    assert "remediation_source=" not in runbook
    assert "docker exec -i" not in runbook
    assert "/root/fbref-production-20260825/README.md" in runbook
    assert 'git cat-file blob "$merged_blob"' in runbook
    assert "same shell" in runbook
    assert "already executes the exact bounded dry-run" in runbook
    assert (
        """run_reviewed_remediation \\
  --competition-id 6 --competition-id 678 \\
  --apply --source-run-id '<owning control run UUID>'"""
        in runbook
    )


def test_runbook_acceptance_allows_only_the_known_comp255_label_alias():
    runbook = RUNBOOK.read_text()
    acceptance = runbook.split("## Post-reconcile acceptance", 1)[1]
    acceptance = acceptance.split(
        "Before any oversized-page requeue", 1
    )[0]

    assert "AS label_mismatches" in acceptance
    assert "AS approved_label_mismatches" in acceptance
    assert "AS unexpected_label_mismatches" in acceptance
    assert "competition_id = '255'" in acceptance
    assert "current_season_id = '2026'" in acceptance
    assert "advertised_source_id = '2026'" in acceptance
    assert ") IS TRUE\n           AS approved_label_mismatch" in acceptance
    assert "AS acceptance_pass" in acceptance
    assert "Expected: `117 / 117 / 117 / 1 / 1 / 0 / 0 / true`." in acceptance


def test_script_supports_exact_stdin_execution_from_runtime_workdir():
    assert remediation._repo_root_for_execution("<stdin>") is None
    result = subprocess.run(
        [sys.executable, "-", "--help"],
        cwd=SCRIPT.parents[2],
        input=SCRIPT.read_text(),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--competition-id" in result.stdout


class FakeControl:
    def __init__(self, competitions, currents, history_records=()):
        self.competitions = list(competitions)
        self.currents = list(currents)
        self.history_records = {
            record.logical_refresh_id: record for record in history_records
        }

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

    def get_current_season_index_evidence(self, competition_id):
        row = next(
            item
            for item in self.competitions
            if item["competition_id"] == competition_id
        )
        if not row.get("last_snapshot_id"):
            raise RuntimeError("missing index snapshot")
        return dict(row["metadata"]["current_season_index"])

    def get_succeeded_fetch_evidence(
        self,
        logical_refresh_id,
        *,
        target_id,
        content_hash,
        raw_manifest_key,
    ):
        record = self.history_records[str(logical_refresh_id)]
        assert record.target_id == target_id
        assert record.content_hash == content_hash
        return {
            "attempt_id": record.attempt_id,
            "run_id": HISTORY_RUN_ID,
            "logical_refresh_id": record.logical_refresh_id,
            "target_id": record.target_id,
            "content_hash": record.content_hash,
            "raw_manifest_key": raw_manifest_key,
        }


def _competition(
    raw,
    competition_id,
    *,
    last_season,
    last_season_url,
    classification="other:national_team",
):
    index_html = f"""
    <h2>National Team Qualification</h2><table><tbody><tr>
      <th data-stat="league_name"><a
        href="/en/comps/{competition_id}/history/x"
      >Competition {competition_id}</a></th>
      <td data-stat="gender">M</td>
      <td data-stat="maxseason"><a href="{last_season_url}">{last_season}</a></td>
    </tr></tbody></table>
    """
    index_record = raw.commit_fetch(
        competition_index_target(),
        index_html.encode(),
        logical_refresh_id=str(uuid.uuid4()),
        attempt_id=str(uuid.uuid4()),
        http_status=200,
    )
    index_snapshot_id = str(uuid.uuid4())
    index_run_id = str(uuid.uuid4())
    index_evidence = {
        "schema_version": "fbref-current-season-index-evidence-v1",
        "snapshot_id": index_snapshot_id,
        "run_id": index_run_id,
        "content_hash": index_record.content_hash,
        "raw": {
            "attempt_id": index_record.attempt_id,
            "content_hash": index_record.content_hash,
            "logical_refresh_id": index_record.logical_refresh_id,
            "manifest_key": raw.fetch_manifest_key(index_record.logical_refresh_id),
            "target_id": index_record.target_id,
        },
        "advertised": {
            "label": last_season,
            "href": last_season_url,
            "season_id": last_season,
        },
    }
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
            "advertised_current_season_id": last_season,
            "current_season_index": index_evidence,
        },
        "last_snapshot_id": index_snapshot_id,
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
        raw,
        "6",
        last_season="2026",
        last_season_url="https://fbref.com/en/comps/6/WCQ----UEFA-M-Stats",
    )
    history_record = _commit_history(
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
        [history_record],
    )

    evidence = remediation.run_remediation(
        control,
        raw,
        competition_ids=["6"],
        apply=False,
    )

    assert evidence["mode"] == "dry_run"
    plan = evidence["plans"][0]
    index = competition["metadata"]["current_season_index"]
    assert plan["competition_id"] == "6"
    assert plan["installed_current_season_id"] == "2022"
    assert plan["advertised_current_season_id"] == "2026"
    assert plan["advertised_current_label"] == "2026"
    assert plan["advertised_current_url"] == (
        "https://fbref.com/en/comps/6/WCQ----UEFA-M-Stats"
    )
    assert plan["action"] == "reconcile"
    assert plan["history_attempt_id"] == history_record.attempt_id
    assert plan["history_run_id"] == HISTORY_RUN_ID
    assert plan["history_content_hash"] == history_record.content_hash
    assert plan["history_logical_refresh_id"] == (history_record.logical_refresh_id)
    assert plan["history_raw_manifest_key"] == raw.fetch_manifest_key(
        history_record.logical_refresh_id
    )
    assert plan["index_snapshot_id"] == index["snapshot_id"]
    assert plan["index_run_id"] == index["run_id"]
    assert plan["index_attempt_id"] == index["raw"]["attempt_id"]
    assert plan["index_content_hash"] == index["content_hash"]
    assert plan["index_raw_manifest_key"] == index["raw"]["manifest_key"]
    assert plan["install_contract_version"] == (
        "fbref-current-season-install-source-link-v1"
    )


def test_comp255_same_source_id_is_no_change_not_demotion(tmp_path):
    raw = RawPageStore.from_uri(tmp_path.as_uri())
    current_url = (
        "https://fbref.com/en/comps/255/2026/2026-FIFA-World-Cup-Qualification-Stats"
    )
    competition = _competition(
        raw,
        "255",
        last_season="2026",
        last_season_url=current_url,
        classification="cup:national_team",
    )
    history_record = _commit_history(
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
        [history_record],
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
        raw,
        "6",
        last_season="2026",
        last_season_url="https://fbref.com/en/comps/6/WCQ----UEFA-M-Stats",
    )
    history_record = _commit_history(
        raw,
        competition,
        """
      <table id="seasons"><tbody><tr><th data-stat="season"><a
        href="/en/comps/6/2022/2022-WCQ----UEFA-M-Stats"
      >2022 FIFA World Cup Qualification — UEFA</a></th></tr></tbody></table>
    """,
    )

    class MissingLineageControl(FakeControl):
        def get_succeeded_fetch_evidence(self, *_args, **_kwargs):
            raise RuntimeError("missing history lineage")

    control = MissingLineageControl(
        [competition],
        [
            _current(
                "6",
                "2022",
                "https://fbref.com/en/comps/6/2022/2022-WCQ----UEFA-M-Stats",
            )
        ],
        [history_record],
    )

    with pytest.raises(
        remediation.RemediationEvidenceError,
        match="history evidence is invalid",
    ):
        remediation.build_remediation_plans(
            control,
            raw,
            competition_ids=["6"],
        )


def test_dry_run_refuses_history_manifest_mismatch(tmp_path):
    raw = RawPageStore.from_uri(tmp_path.as_uri())
    competition = _competition(
        raw,
        "6",
        last_season="2026",
        last_season_url="https://fbref.com/en/comps/6/WCQ----UEFA-M-Stats",
    )
    history_record = _commit_history(
        raw,
        competition,
        """
        <table id="seasons"><tbody><tr><th data-stat="season"><a
          href="/en/comps/6/2022/old">2022</a>
        </th></tr></tbody></table>
        """,
    )

    class WrongManifestControl(FakeControl):
        def get_succeeded_fetch_evidence(self, *args, **kwargs):
            result = super().get_succeeded_fetch_evidence(*args, **kwargs)
            result["raw_manifest_key"] = "manifests/fetches/wrong.json"
            return result

    control = WrongManifestControl(
        [competition],
        [_current("6", "2022", "https://fbref.com/en/comps/6/2022/old")],
        [history_record],
    )

    with pytest.raises(
        remediation.RemediationEvidenceError,
        match="history evidence is not exact",
    ):
        remediation.build_remediation_plans(
            control,
            raw,
            competition_ids=["6"],
        )


def test_dry_run_refuses_missing_index_snapshot(tmp_path):
    raw = RawPageStore.from_uri(tmp_path.as_uri())
    competition = _competition(
        raw,
        "6",
        last_season="2026",
        last_season_url="https://fbref.com/en/comps/6/WCQ----UEFA-M-Stats",
    )
    competition["last_snapshot_id"] = None

    with pytest.raises(
        remediation.RemediationEvidenceError,
        match="index evidence is invalid",
    ):
        remediation.build_remediation_plans(
            FakeControl(
                [competition],
                [_current("6", "2022", "https://fbref.com/en/comps/6/2022/old")],
            ),
            raw,
            competition_ids=["6"],
        )


def test_requested_competition_requires_exactly_one_current_season(tmp_path):
    raw = RawPageStore.from_uri(tmp_path.as_uri())
    competition = _competition(
        raw,
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


def test_apply_dispatches_one_supported_atomic_batch(tmp_path):
    raw = RawPageStore.from_uri(tmp_path.as_uri())
    competitions = []
    currents = []
    history_records = []
    for competition_id in ("6", "678"):
        competition = _competition(
            raw,
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
        history_records.append(
            _commit_history(
                raw,
                competition,
                f"""
          <table id="seasons"><tbody><tr><th data-stat="season"><a
            href="/en/comps/{competition_id}/2022/Old-Stats"
          >2022 Old season</a></th></tr></tbody></table>
        """,
            )
        )

    calls = []

    class AtomicPipeline:
        def __init__(self, control, raw_store):
            self.control = control
            self.raw_store = raw_store

        def remediate_current_seasons(self, items):
            calls.append(list(items))
            return {"competition_count": len(items), "seeded": 2, "skipped": 0}

    result = remediation.run_remediation(
        FakeControl(competitions, currents, history_records),
        raw,
        competition_ids=["6", "678"],
        apply=True,
        source_run_id=HISTORY_RUN_ID,
        pipeline_factory=AtomicPipeline,
    )

    assert result["mode"] == "apply"
    assert len(calls) == 1
    assert [item.evidence.competition_id for item in calls[0]] == ["6", "678"]
    assert "_parse_competition" not in SCRIPT.read_text()


def test_apply_refuses_source_run_mismatch_before_atomic_operation(tmp_path):
    raw = RawPageStore.from_uri(tmp_path.as_uri())
    competition = _competition(
        raw,
        "6",
        last_season="2026",
        last_season_url="https://fbref.com/en/comps/6/WCQ----UEFA-M-Stats",
    )
    history_record = _commit_history(
        raw,
        competition,
        """
        <table id="seasons"><tbody><tr><th data-stat="season"><a
          href="/en/comps/6/2022/old">2022</a>
        </th></tr></tbody></table>
        """,
    )

    class ForbiddenPipeline:
        def __init__(self, *_args):
            raise AssertionError("atomic operation must not begin")

    with pytest.raises(
        remediation.RemediationEvidenceError,
        match="source_run_id does not own exact history evidence",
    ):
        remediation.run_remediation(
            FakeControl(
                [competition],
                [_current("6", "2022", "https://fbref.com/en/comps/6/2022/old")],
                [history_record],
            ),
            raw,
            competition_ids=["6"],
            apply=True,
            source_run_id=str(uuid.uuid4()),
            pipeline_factory=ForbiddenPipeline,
        )


@pytest.mark.parametrize("competition_ids", [[], ["6", "6"]])
def test_scope_must_be_explicit_and_unique(tmp_path, competition_ids):
    with pytest.raises(remediation.RemediationConfigurationError):
        remediation.run_remediation(
            FakeControl([], []),
            RawPageStore.from_uri(tmp_path.as_uri()),
            competition_ids=competition_ids,
            apply=False,
        )
