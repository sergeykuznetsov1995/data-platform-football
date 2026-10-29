"""Offline weekly completeness and output contract regressions (#1336)."""

from datetime import date, datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.report_fbref_daily import main
from scrapers.fbref.daily_report_reader import READINESS_SQL, collect_snapshot

AS_OF = datetime(2026, 10, 12, 12, tzinfo=timezone.utc)
MAP = {"9": {"fotmob_ids": ["47"]}}


def snapshot():
    data = {
        "competitions": [{"competition_id": "9", "name": "Premier League", "gender": "male",
                          "crawl_state": "active", "present": True, "lifecycle_state": "present"}],
        "seasons": [{"competition_id": "9", "season_id": "2026-2027", "is_current": True,
                     "present": True, "lifecycle_state": "present"}],
        "schedule_health": [{"competition_id": "9", "season_id": "2026-2027", "fetched": True}],
        "coverage": {"start": "2026-10-05", "end": "2026-10-11", "as_of": AS_OF.isoformat()},
        "fotmob_seasons": [{"competition_id": "47", "season_key": "2026/2027",
                            "fetched_at": "2026-10-12T11:00:00Z"}],
        "schedules": [], "observations": [], "readiness": [], "fotmob_matches": [],
    }
    for i, kickoff in enumerate(["2026-10-05T00:00:00Z", "2026-10-07T17:00:00Z", "2026-10-11T23:59:59Z"]):
        mid = f"{i:08x}"
        data["schedules"].append({"source_competition_id": "9", "source_season_id": "2026-2027",
                                  "date": kickoff[:10], "score": "2–1", "match_url": f"/en/matches/{mid}/Test"})
        data["observations"].append({"competition_id": "9", "season_id": "2026-2027", "match_id": mid,
                                     "kickoff_at": kickoff, "first_completed_seen_at": kickoff})
        data["readiness"].append({"competition_id": "9", "season_id": "2026-2027", "match_id": mid,
                                  "bronze_ready_at": "2026-10-09T20:00:00Z" if i < 2 else None})
        data["fotmob_matches"].append({"competition_id": "47", "season_key": "2026/2027", "match_id": str(i),
                                      "utc_time": kickoff, "finished": True})
    return data


def weekly(data=None):
    from scrapers.fbref.weekly_report import build_weekly_report
    return build_weekly_report(data or snapshot(), as_of=AS_OF, mapping=MAP)


def test_weekly_counts_ready_bronze_including_late_instead_of_schedule():
    data = snapshot()
    for key in ("schedules", "fotmob_matches"):
        data[key] *= 2
    row = weekly(data)["competitions"][0]
    assert (row["fotmob"], row["bronze_ready"], row["difference"]) == (3, 2, -1)
    assert row["mapping"]["basis"] == "explicit_id_map"
    assert row["mapping"]["confidence"] == "not_assessed"
    from scrapers.fbref.weekly_report import completed_week
    assert completed_week(datetime.fromisoformat("2026-10-12T01:00:00+02:00")) == (date(2026, 9, 28), date(2026, 10, 5))


@pytest.mark.parametrize("ready", [None, "2026-10-12T13:00:00Z", "2026-10-04T23:59:59Z"])
def test_future_preview_and_unfinished_typed_are_not_ready(ready):
    data = snapshot()
    data["readiness"][0]["bronze_ready_at"] = ready
    assert weekly(data)["competitions"][0]["bronze_ready"] == 1


@pytest.mark.parametrize("kickoff", ["2026-10-04T23:59:59Z", "2026-10-12T00:00:00Z"])
def test_week_boundaries_exclude_other_weeks(kickoff):
    data = snapshot()
    data["observations"][0]["kickoff_at"] = kickoff
    data["schedules"][0]["date"] = kickoff[:10]
    data["fotmob_matches"][0]["utc_time"] = kickoff
    row = weekly(data)["competitions"][0]
    assert (row["fotmob"], row["bronze_ready"]) == (2, 1)


@pytest.mark.parametrize("failure", ["coverage", "short_start", "short_end", "cutoff", "stale", "error", "utc", "fbref_utc", "season", "schedule", "phase", "multiple_seasons"])
def test_missing_evidence_is_not_zero_deficit(failure):
    data = snapshot()
    if failure == "coverage":
        data.pop("coverage")
    elif failure == "short_start":
        data["coverage"]["start"] = "2026-10-06"
    elif failure == "short_end":
        data["coverage"]["end"] = "2026-10-10"
    elif failure == "schedule":
        data["schedule_health"][0]["fetched"] = False
    elif failure == "phase":
        data["fotmob_seasons"][0]["season_key"] = "2026/2027 - Unknown"
    elif failure == "multiple_seasons":
        data["seasons"].append(dict(data["seasons"][0], season_id="2025-2026"))
        data["observations"].append(dict(data["observations"][0], season_id="2025-2026"))
    elif failure == "cutoff":
        data["coverage"]["as_of"] = "2026-10-12T11:00:00Z"
    elif failure == "stale":
        data["fotmob_seasons"][0]["stale"] = True
    elif failure == "error":
        data["errors"] = [{"source": "fbref_readiness", "reason": "read_failed"}]
    elif failure == "utc":
        data["fotmob_matches"][0]["utc_time"] = None
    elif failure == "fbref_utc":
        data["observations"][0]["kickoff_at"] = None
    else:
        data["seasons"] = []
    assert weekly(data)["competitions"][0]["difference"] is None


def test_unmapped_without_season_is_visible_but_non_adult_and_retired_are_excluded():
    data = snapshot()
    base = data["competitions"][0]
    data["competitions"] += [dict(base, competition_id="10", name="Unknown"),
                             dict(base, competition_id="11", name="Women", gender="female"),
                             dict(base, competition_id="12", name="U21 League"),
                             dict(base, competition_id="13", crawl_state="skipped", metadata={
                                 "current_scope_lifecycle": "discontinued", "current_scope_reason": "closed"})]
    result = weekly(data)
    assert {r["competition_id"] for r in result["competitions"]} == {"9", "10"}
    assert result["unmapped"][0]["competition_id"] == "10"
    assert result["unmapped"][0]["reason"] == "unmapped_fotmob_competition"
    assert result["competitions"][0]["difference"] is None


def test_reader_covers_week_even_with_short_lookback():
    data = snapshot()
    queries = []
    def pg(sql):
        return {k: data[k] for k in ("competitions", "seasons", "schedule_health")} if "'competitions'" in sql else []
    def trino(sql):
        queries.append(sql)
        return []
    collected = collect_snapshot(pg, trino, day=date(2026, 10, 11), as_of=AS_OF, mapping=MAP, lookback=1)
    assert collected["coverage"] == data["coverage"]
    schedule = next(q for q in queries if "bronze.fbref_schedule" in q)
    fixtures = next(q for q in queries if "bronze.fotmob_matches_current" in q)
    assert "2026-10-05" in schedule and "2026-10-05" in fixtures
    assert "t.content_hash = p.content_hash" in READINESS_SQL
    assert "p.content_hash = a.content_hash" in READINESS_SQL
    for alias in ("p", "t"):
        for field in ("parse_status", "persistence_status", "validation_status"):
            assert f"{alias}.{field} = 'succeeded'" in READINESS_SQL
        assert f"{alias}.completed_at >= o.first_completed_seen_at" in READINESS_SQL
        assert f"{alias}.completed_at <= '{{cutoff}}'" in READINESS_SQL


def test_offline_cli_json_markdown_and_adapter_keep_entire_week(tmp_path, capsys, monkeypatch):
    data = snapshot()
    data["competitions"].append(dict(data["competitions"][0], competition_id="10", name="Unknown"))
    source = tmp_path / "snapshot.json"
    source.write_text(json.dumps(data))
    mapping = tmp_path / "map.json"
    mapping.write_text(json.dumps({"competitions": MAP}))
    output = tmp_path / "output"
    assert main(["--input", str(source), "--map", str(mapping), "--as-of", AS_OF.isoformat(),
                 "--lookback", "1", "--output-dir", str(output)]) == 0
    lines = capsys.readouterr().out.splitlines()
    bundle = json.loads(next(p for p in output.glob("*.json") if "snapshot" not in p.name).read_text())
    assert bundle["weekly"]["competitions"][1]["difference"] == -1
    markdown = next(output.glob("*.md")).read_text()
    assert all(line in markdown for line in lines[4:])
    root = Path(__file__).resolve().parents[3]
    spec = importlib.util.spec_from_file_location("weekly_adapter", root / "deploy/fbref/morning_report_adapter.py")
    adapter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(adapter)
    monkeypatch.setenv("FBREF_DAILY_REPORT_ROOT", str(root))
    monkeypatch.setattr(adapter.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout="\n".join(lines)))
    assert adapter.fbref_daily_report() == ["• " + line for line in lines]
    lines.pop()
    assert "недоступен" in adapter.fbref_daily_report()[0]


def test_duplicate_reference_identity_on_different_days_is_not_two_matches():
    data = snapshot()
    data["fotmob_matches"].append(dict(data["fotmob_matches"][0], utc_time="2026-10-06T00:00:00Z"))
    row = weekly(data)["competitions"][0]
    assert row["bronze_ready"] == 2
    assert row["fotmob"] is None and row["difference"] is None
    assert {p["reason"] for p in row["evidence"]} == {"conflicting_fotmob_fixture_dates"}


@pytest.mark.parametrize("match_id", ["0", "other"])
def test_reference_identity_across_seasons_on_same_day(match_id):
    data = snapshot()
    for key in ("schedules", "observations", "readiness", "fotmob_matches"):
        data[key] = data[key][:1]
    data["seasons"].append(dict(data["seasons"][0], season_id="2027-2028"))
    data["schedule_health"].append(dict(data["schedule_health"][0], season_id="2027-2028"))
    data["fotmob_seasons"].append(dict(data["fotmob_seasons"][0], season_key="2027/2028"))
    data["fotmob_matches"].append(dict(data["fotmob_matches"][0],
        season_key="2027/2028", match_id=match_id))
    row = weekly(data)["competitions"][0]
    if match_id == "0":
        assert (row["fotmob"], row["bronze_ready"], row["difference"]) == (None, None, None)
        assert {p["reason"] for p in row["evidence"]} == {"season_conflict"}
    else:
        assert (row["fotmob"], row["bronze_ready"], row["difference"]) == (2, 1, -1)
        assert row["evidence"] == []


@pytest.mark.parametrize("scenario", ["fotmob", "fbref", "both", "cross_source", "ordinary", "same_season"])
def test_weekly_match_identity_and_season_conflict_on_either_side(scenario):
    data = snapshot()
    for key in ("schedules", "observations", "readiness", "fotmob_matches"):
        data[key] = data[key][:1]
    data["fotmob_matches"][0]["match_id"] = data["observations"][0]["match_id"]
    data["seasons"].append(dict(data["seasons"][0], season_id="2027-2028"))
    data["schedule_health"].append(dict(data["schedule_health"][0], season_id="2027-2028"))
    data["fotmob_seasons"].append(dict(data["fotmob_seasons"][0], season_key="2027/2028"))
    if scenario in {"fotmob", "both"}:
        data["fotmob_matches"].append(dict(data["fotmob_matches"][0], season_key="2027/2028"))
    if scenario in {"fbref", "both"}:
        data["schedules"].append(dict(data["schedules"][0], source_season_id="2027-2028"))
        for key in ("observations", "readiness"):
            data[key].append(dict(data[key][0], season_id="2027-2028"))
    if scenario == "cross_source":
        data["fotmob_matches"][0]["season_key"] = "2027/2028"
    if scenario == "same_season":
        for key in ("schedules", "observations", "readiness", "fotmob_matches"):
            data[key] *= 2
    row = weekly(data)["competitions"][0]
    # match_id is a per-source namespace: equal strings across sources are not one match.
    if scenario in {"ordinary", "same_season", "cross_source"}:
        assert (row["fotmob"], row["bronze_ready"], row["difference"]) == (1, 1, 0)
        assert row["evidence"] == []
    else:
        assert (row["fotmob"], row["bronze_ready"], row["difference"]) == (None, None, None)
        assert {p["reason"] for p in row["evidence"]} == {"season_conflict"}


@pytest.mark.parametrize("fbref_fixtures", [True, False])
def test_weekly_keeps_previous_season_after_registry_rollover(fbref_fixtures):
    data = snapshot()
    data["seasons"][0]["is_current"] = False
    data["seasons"].append(dict(data["seasons"][0], season_id="2027-2028", is_current=True))
    data["schedule_health"].append(dict(data["schedule_health"][0], season_id="2027-2028"))
    data["fotmob_seasons"].append(dict(data["fotmob_seasons"][0], season_key="2027/2028"))
    # The new edition is fresh but its fixtures belong to the following week.
    data["fotmob_matches"].append(dict(data["fotmob_matches"][0],
        season_key="2027/2028", match_id="new", utc_time="2026-10-12T01:00:00Z"))
    if not fbref_fixtures:
        data["schedules"] = data["observations"] = data["readiness"] = []
    row = weekly(data)["competitions"][0]
    assert (row["fotmob"], row["bronze_ready"], row["difference"]) == (
        (3, 2, -1) if fbref_fixtures else (3, 0, -3))
    assert row["evidence"] == []
    # Weekly selection must not mutate the daily meter's current scope.
    from scrapers.fbref.daily_report import build_daily_report
    daily = build_daily_report(data, day=date(2026, 10, 7), as_of=AS_OF, mapping=MAP)
    assert daily["played"] == 0
    assert data["seasons"][0]["is_current"] is False


@pytest.mark.parametrize("old_present", [True, False])
def test_weekly_sums_both_editions_when_week_crosses_rollover(old_present):
    data = snapshot()
    data["seasons"][0]["is_current"] = False
    data["seasons"].append(dict(data["seasons"][0], season_id="2027-2028", is_current=True))
    data["schedule_health"].append(dict(data["schedule_health"][0], season_id="2027-2028"))
    data["fotmob_seasons"].append(dict(data["fotmob_seasons"][0], season_key="2027/2028"))
    for key in ("observations", "readiness"):
        data[key][-1]["season_id"] = "2027-2028"
    data["schedules"][-1]["source_season_id"] = "2027-2028"
    data["fotmob_matches"][-1]["season_key"] = "2027/2028"
    data["seasons"][0]["present"] = old_present
    row = weekly(data)["competitions"][0]
    if old_present:
        assert (row["fotmob"], row["bronze_ready"], row["difference"]) == (3, 2, -1)
        assert row["evidence"] == []
    else:
        assert row["difference"] is None
        assert {p["reason"] for p in row["evidence"]} == {"missing_or_unavailable_week_season"}


@pytest.mark.parametrize("fbref_fixtures", [True, False])
@pytest.mark.parametrize("season_key", ["2026/2027", "2026/2027 - Unknown"])
def test_weekly_reports_previous_season_missing_from_registry(fbref_fixtures, season_key):
    data = snapshot()
    data["seasons"][0]["season_id"] = "2027-2028"
    data["schedule_health"][0]["season_id"] = "2027-2028"
    data["fotmob_seasons"].append(dict(data["fotmob_seasons"][0], season_key="2027/2028"))
    for row in data["fotmob_matches"]:
        row["season_key"] = season_key
    if not fbref_fixtures:
        data["schedules"] = data["observations"] = data["readiness"] = []
    row = weekly(data)["competitions"][0]
    assert row["fotmob"] is None and row["bronze_ready"] is None and row["difference"] is None
    assert {"competition_id": "9", "season_id": "2026-2027",
            "reason": "missing_or_unavailable_week_season"} in row["evidence"]
    assert data["seasons"][0]["is_current"] is True


def test_reader_retains_noncurrent_registry_and_manifest_proofs():
    queries = []
    data = snapshot()
    def pg(sql):
        queries.append(sql)
        if "'competitions'" in sql:
            return {k: data[k] for k in ("competitions", "seasons", "schedule_health")}
        if "jsonb_agg(o)" in sql:
            return data["observations"]
        if "jsonb_agg(r)" in sql:
            return data["readiness"]
        return []
    collected = collect_snapshot(pg, lambda sql: [], day=date(2026, 10, 11),
                                 as_of=AS_OF, mapping=MAP, lookback=1)
    registry, observations, readiness = queries[:3]
    assert "AND is_current" not in registry
    assert "WHERE s.is_current" not in registry
    assert "WHERE s.is_current" not in observations
    assert "WHERE s.is_current" not in readiness
    assert collected["observations"] == data["observations"]
    assert collected["readiness"] == data["readiness"]


@pytest.mark.parametrize("failure", ["utc", "phase", "stale", "schedule"])
def test_rollover_keeps_previous_edition_evidence_gaps(failure):
    data = snapshot()
    data["seasons"][0]["is_current"] = False
    data["seasons"].append(dict(data["seasons"][0], season_id="2027-2028", is_current=True))
    data["schedule_health"].append(dict(data["schedule_health"][0], season_id="2027-2028"))
    data["fotmob_seasons"].append(dict(data["fotmob_seasons"][0], season_key="2027/2028"))
    data["schedules"] = data["observations"] = data["readiness"] = []
    if failure == "utc":
        for row in data["fotmob_matches"]:
            row["utc_time"] = None
    elif failure == "phase":
        for row in data["fotmob_matches"]:
            row["season_key"] = "2026/2027 - Unknown"
    elif failure == "stale":
        data["fotmob_seasons"][0]["stale"] = True
    else:
        data["schedule_health"][0]["fetched"] = False
    row = weekly(data)["competitions"][0]
    assert row["fotmob"] is None and row["difference"] is None
    assert row["evidence"]


@pytest.mark.parametrize("side", ["fbref", "fotmob"])
def test_season_conflict_is_checked_before_played_and_finished_filters(side):
    data = snapshot()
    for key in ("schedules", "observations", "readiness", "fotmob_matches"):
        data[key] = data[key][:1]
    data["fotmob_matches"][0]["match_id"] = data["observations"][0]["match_id"]
    data["seasons"].append(dict(data["seasons"][0], season_id="2027-2028"))
    data["schedule_health"].append(dict(data["schedule_health"][0], season_id="2027-2028"))
    data["fotmob_seasons"].append(dict(data["fotmob_seasons"][0], season_key="2027/2028"))
    if side == "fbref":
        data["schedules"].append(dict(data["schedules"][0], source_season_id="2027-2028", score=None))
    else:
        data["fotmob_matches"].append(dict(data["fotmob_matches"][0], season_key="2027/2028", finished=False))
    row = weekly(data)["competitions"][0]
    assert (row["fotmob"], row["bronze_ready"], row["difference"]) == (None, None, None)
    assert {p["reason"] for p in row["evidence"]} == {"season_conflict"}
