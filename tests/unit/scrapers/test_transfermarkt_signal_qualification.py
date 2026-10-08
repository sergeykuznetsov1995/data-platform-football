"""Concrete offline evidence fixtures; no boolean-only qualification shortcut."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from scrapers.transfermarkt.signal_qualification import (
    GROUPS, OBSERVATION_SCHEMA, QualificationError, build_qualification,
    validate_qualification,
)
from scrapers.transfermarkt.tmapi import players_url


def qualified_fixture(tmp_path, *, changes=True, players=1000):
    """Reusable SYNTHETIC source observations for runner tests, never runtime proof."""
    root = Path(tmp_path) / "qualification"
    root.mkdir(parents=True, exist_ok=True)
    ids = [str(player) for player in range(1, players + 1)]
    cohort = {"player_ids": ids, "groups": sorted(GROUPS), "scope_basis": "configured_denominator", "source_current_editions_proven": False, "scopes": [f"TEST{scope}/2026" for scope in range(20)]}
    paths = []
    batch_urls = {start // 300: players_url(ids[start:start + 300]) for start in range(0, players, 300)}
    for day in (10, 11):
        prefix = f"2026-10-{day:02d}T10:00:"
        rows = []
        for offset, player in enumerate(ids):
            club = str(100 + offset % 20)
            changed = changes and day == 11 and player == "1"
            value = 2000 if changed else 1000
            value_date = "2026-10-11" if changed else "2026-10-10"
            api = {"value": value, "value_date": value_date, "value_present": True, "contract": "2028-06-30", "clubs": [club]}
            urls = {"tmapi": batch_urls[offset // 300], "plus1": f"https://www.transfermarkt.com/test/kader/verein/{club}/plus/1",
                    "mv": f"https://www.transfermarkt.com/ceapi/marketValueDevelopment/graph/{player}",
                    "transfers": f"https://www.transfermarkt.com/ceapi/transferHistory/list/{player}"}
            raw = {endpoint: {"capture_id": hashlib.sha256(f"synthetic/{day}/{endpoint}/{player}".encode()).hexdigest(),
                              "body_sha256": hashlib.sha256(f"synthetic-body/{day}/{endpoint}/{player}".encode()).hexdigest(),
                              "url": url, "fetched_at": prefix + "01+00:00"} for endpoint, url in urls.items()}
            rows.append({"player_id": player, "tmapi": api, "plus1": {"value": value, "contract": "2028-06-30", "club_id": club},
                         "ceapi": {"mv": {"status": "compared", "value": value, "value_date": value_date, "raw_rows": 1},
                                   "transfers": {"status": "compared", "club_id": club, "raw_rows": 1}}, "raw": raw})
        observation = {"schema_version": OBSERVATION_SCHEMA, "players": rows, "missing_ids": [], "unknown_ids": [],
                       "observed_from": prefix + "00+00:00", "observed_to": prefix + "03+00:00", "day_msk": f"2026-10-{day:02d}",
                       "ceapi_empty_counts": {"mv": 0, "transfers": 0}, "counts": {"expected": players, "compared": players, "missing": 0, "unknown": 0}}
        path = root / f"day{day-9}.json"
        path.write_text(json.dumps({"mode": "parity", "complete": True, "task_failures": {}, "players_checked": players, "cohort": cohort, "qualification_observation": observation}))
        paths.append(path)
    if not changes or players < 1000:
        # Keep the real rejected artifact available for negative runner tests.
        from scrapers.transfermarkt.signal_qualification import SCHEMA
        artifact = {"schema_version": SCHEMA, "cohort": cohort, "reports": [{"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for path in paths]}
        output = root / "evidence.json"
        output.write_text(json.dumps(artifact))
        return {"evidence_path": str(output), "evidence_sha256": hashlib.sha256(output.read_bytes()).hexdigest()}
    return build_qualification(paths[0], paths[1], root / "evidence.json")


def mutate_report(wrapper, change):
    path = Path(wrapper["evidence_path"])
    artifact = json.loads(path.read_text())
    report_path = path.parent / artifact["reports"][1]["path"]
    report = json.loads(report_path.read_text())
    change(report)
    report_path.write_text(json.dumps(report))
    artifact["reports"][1]["sha256"] = hashlib.sha256(report_path.read_bytes()).hexdigest()
    path.write_text(json.dumps(artifact))
    return {"evidence_path": str(path), "evidence_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "source_parity": True, "same_day_changes": True, "players_compared": 1000}


def test_complete_concrete_reports_qualify_and_ignore_wrapper_boolean_flags(tmp_path):
    wrapper = qualified_fixture(tmp_path)
    wrapper.update(source_parity=False, same_day_changes=False, players_compared=1)
    result = validate_qualification(wrapper)
    assert result["players_compared"] == 1000 and result["source_parity"] and result["same_day_changes"]
    assert {change["field"] for change in result["changed_observations"]} == {"value", "value_date"}


@pytest.mark.parametrize("changes,players", [(False, 1000), (True, 999)])
def test_no_real_change_or_insufficient_cohort_never_qualifies(tmp_path, changes, players):
    wrapper = qualified_fixture(tmp_path, changes=changes, players=players)
    wrapper.update(source_parity=True, same_day_changes=True, players_compared=1000)
    with pytest.raises(QualificationError):
        validate_qualification(wrapper)


@pytest.mark.parametrize("change", [
    lambda report: report["qualification_observation"]["players"][0]["plus1"].update(contract="2029-06-30"),
    lambda report: report["qualification_observation"]["players"][0]["ceapi"]["mv"].update(value=9999),
    lambda report: report["qualification_observation"]["players"][0]["ceapi"]["transfers"].update(status="unknown"),
    lambda report: report["qualification_observation"].update(missing_ids=["1"]),
    lambda report: report["qualification_observation"]["players"].pop(),
    lambda report: report["qualification_observation"]["players"][0]["raw"]["tmapi"].update(url="https://tmapi.transfermarkt.technology/players?ids[]=99999"),
    lambda report: report["qualification_observation"].update(ceapi_empty_counts={"mv": 1, "transfers": 0}),
])
def test_consistent_file_hashes_do_not_hide_bad_concrete_source_evidence(tmp_path, change):
    wrapper = mutate_report(qualified_fixture(tmp_path), change)
    with pytest.raises(QualificationError):
        validate_qualification(wrapper)


def test_changed_signal_seen_next_moscow_day_fails(tmp_path):
    def alter(report):
        observation = report["qualification_observation"]
        observation.update(observed_from="2026-10-11T20:00:00+00:00", observed_to="2026-10-11T23:00:00+00:00", day_msk="2026-10-12")
        for row in observation["players"]:
            for proof in row["raw"].values():
                proof["fetched_at"] = "2026-10-11T20:30:00+00:00"
        observation["players"][0]["raw"]["tmapi"]["fetched_at"] = "2026-10-11T23:00:00+00:00"
    wrapper = qualified_fixture(tmp_path)
    artifact_path = Path(wrapper["evidence_path"])
    artifact = json.loads(artifact_path.read_text())
    first_path = artifact_path.parent / artifact["reports"][0]["path"]
    first = json.loads(first_path.read_text())
    observation = first["qualification_observation"]
    observation.update(observed_from="2026-10-11T05:00:00+00:00", observed_to="2026-10-11T05:00:03+00:00", day_msk="2026-10-11")
    for row in observation["players"]:
        for proof in row["raw"].values():
            proof["fetched_at"] = "2026-10-11T05:00:01+00:00"
    first_path.write_text(json.dumps(first))
    artifact["reports"][0]["sha256"] = hashlib.sha256(first_path.read_bytes()).hexdigest()
    artifact_path.write_text(json.dumps(artifact))
    wrapper["evidence_sha256"] = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    with pytest.raises(QualificationError, match="same day"):
        validate_qualification(mutate_report(wrapper, alter))


def test_simple_boolean_blob_with_matching_hash_is_rejected(tmp_path):
    path = tmp_path / "fake.json"
    path.write_text(json.dumps({"players_compared": 1000, "source_parity": True, "same_day_changes": True}))
    with pytest.raises(QualificationError, match="schema"):
        validate_qualification({"players_compared": 1000, "source_parity": True, "same_day_changes": True,
                                "evidence_path": str(path), "evidence_sha256": hashlib.sha256(path.read_bytes()).hexdigest()})


def test_builder_measured_recheck_repairs_only_known_changed_subset(tmp_path):
    wrapper = qualified_fixture(tmp_path)
    artifact = json.loads(Path(wrapper["evidence_path"]).read_text())
    day1, day2 = [Path(wrapper["evidence_path"]).parent / link["path"] for link in artifact["reports"]]
    original = json.loads(day2.read_text())
    checked_row = deepcopy(original["qualification_observation"]["players"][0])
    for proof in checked_row["raw"].values():
        proof["fetched_at"] = "2026-10-11T10:05:01+00:00"
    recheck = {"mode": "recheck", "complete": True, "task_failures": {}, "original_cohort": artifact["cohort"],
               "selected_changed_ids": ["1"], "unrechecked_changed_ids": [],
               "qualification_observation": {"schema_version": OBSERVATION_SCHEMA, "players": [checked_row], "missing_ids": [], "unknown_ids": [],
                                             "observed_from": "2026-10-11T10:05:00+00:00", "observed_to": "2026-10-11T10:05:03+00:00", "day_msk": "2026-10-11",
                                             "ceapi_empty_counts": {"mv": 0, "transfers": 0}, "counts": {"expected": 1, "compared": 1, "missing": 0, "unknown": 0}}}
    recheck_path = tmp_path / "recheck.json"
    recheck_path.write_text(json.dumps(recheck))
    # Day2 initially caught a real change, but its earlier career reference had
    # not caught up. The measured repeat has its own later raw proof.
    original["qualification_observation"]["players"][0]["ceapi"]["mv"]["value"] = 1000
    day2.write_text(json.dumps(original))
    with pytest.raises(QualificationError, match="market value"):
        build_qualification(day1, day2, tmp_path / "without-recheck.json")
    qualified = build_qualification(day1, day2, tmp_path / "with-recheck.json", recheck=recheck_path)
    assert qualified["players_compared"] == 1000
    assert all(change["player_id"] == "1" for change in qualified["changed_observations"])
    assert all("10:05" in change["signal_fetched_at"] for change in qualified["changed_observations"])


def test_measured_recheck_cannot_invent_a_change_on_unchanged_id(tmp_path):
    wrapper = qualified_fixture(tmp_path)
    artifact = json.loads(Path(wrapper["evidence_path"]).read_text())
    paths = [Path(wrapper["evidence_path"]).parent / link["path"] for link in artifact["reports"]]
    original = json.loads(paths[1].read_text())
    row = deepcopy(original["qualification_observation"]["players"][1])
    recheck = {"mode": "recheck", "complete": True, "task_failures": {}, "original_cohort": artifact["cohort"], "selected_changed_ids": ["2"],
               "qualification_observation": {**original["qualification_observation"], "players": [row], "counts": {"expected": 1, "compared": 1, "missing": 0, "unknown": 0}}}
    recheck_path = tmp_path / "fake-change.json"
    recheck_path.write_text(json.dumps(recheck))
    with pytest.raises(QualificationError, match="actual changed"):
        build_qualification(paths[0], paths[1], tmp_path / "evidence.json", recheck=recheck_path)
