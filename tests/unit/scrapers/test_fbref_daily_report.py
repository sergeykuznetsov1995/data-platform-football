"""Behavioral tests for daily cohort, deadlines, observations and referee."""

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from scrapers.fbref.daily_report import (
    adult_competition, build_daily_report, milestone_streak, percentile95,
    render_markdown, season_token,
)
from scrapers.fbref.daily_report_reader import collect_snapshot
from scrapers.fbref.report_observations import schedule_observations

DAY = date(2026, 10, 7)
ANCHOR = datetime(2026, 10, 7, 20, tzinfo=timezone.utc)
AS_OF = ANCHOR + timedelta(hours=30)
MAP = {"9": {"fotmob_ids": ["47"]}}


def snapshot():
    return {
        "competitions": [{"competition_id": "9", "name": "Premier League", "gender": "male",
                          "classification": "league:club", "crawl_state": "active",
                          "present": True, "lifecycle_state": "present"}],
        "seasons": [{"competition_id": "9", "season_id": "2026-2027", "is_current": True,
                     "present": True, "lifecycle_state": "present"}],
        "schedule_health": [{"competition_id": "9", "season_id": "2026-2027", "fetched": True}],
        "schedules": [{"source_competition_id": "9", "source_season_id": "2026-2027",
                       "date": "2026-10-07", "time": "17:00", "home": "Home", "away": "Away",
                       "score": "2–1", "match_url": "https://fbref.com/en/matches/12345678/Test"}],
        "observations": [{"competition_id": "9", "season_id": "2026-2027", "match_id": "12345678",
                          "first_seen_at": ANCHOR.isoformat(), "first_completed_seen_at": ANCHOR.isoformat(),
                          "first_seen_raw_key": "raw/first", "first_completed_raw_key": "raw/final",
                          "kickoff_at": (ANCHOR - timedelta(hours=3)).isoformat()}],
        "readiness": [{"competition_id": "9", "season_id": "2026-2027", "match_id": "12345678",
                       "first_fetch_at": (ANCHOR + timedelta(hours=1)).isoformat(),
                       "bronze_ready_at": (ANCHOR + timedelta(hours=24)).isoformat()}],
        "fotmob_seasons": [{"competition_id": "47", "season_key": "2026/2027",
                            "fetched_at": (AS_OF - timedelta(hours=1)).isoformat()}],
        "fotmob_matches": [{"competition_id": "47", "season_key": "2026/2027", "match_id": "1000",
                            "utc_time": (ANCHOR - timedelta(hours=3)).isoformat(), "finished": True,
                            "timezone": ""}],
    }


def report(data=None, cutoff=AS_OF):
    return build_daily_report(data or snapshot(), day=DAY, as_of=cutoff, mapping=MAP)


def test_exact_24_hours_is_success_and_fetch_is_not_bronze_readiness():
    result = report()
    assert (result["played"], result["on_time"], result["fotmob"]) == (1, 1, 1)
    assert result["final"] and result["milestone_day_pass"]
    assert result["source_lag"]["median_hours"] == 1
    assert result["match_to_first_fetch"]["median_hours"] == 2
    assert result["match_to_bronze"]["median_hours"] == 25
    data = snapshot()
    data["readiness"][0]["bronze_ready_at"] = (ANCHOR + timedelta(hours=24, seconds=1)).isoformat()
    assert report(data)["late"] == 1


def test_pending_cohort_is_provisional_even_if_already_collected():
    result = report(cutoff=ANCHOR + timedelta(hours=12))
    assert result["pending"] == 1 and not result["final"]
    data = snapshot()
    data["readiness"][0]["bronze_ready_at"] = (ANCHOR + timedelta(hours=2)).isoformat()
    result = report(data, cutoff=ANCHOR + timedelta(hours=12))
    assert result["on_time"] == 1 and not result["final"]


def test_fetch_without_parse_persist_validation_proof_is_late():
    data = snapshot()
    data["readiness"][0]["bronze_ready_at"] = None
    result = report(data)
    assert result["late"] == 1 and result["violators"][0]["first_fetch_at"]
    assert not result["milestone_day_pass"]


def test_missing_report_and_unknown_first_seen_remain_in_denominator():
    data = snapshot()
    data["schedules"][0]["match_url"] = None
    assert report(data)["played"] == 1
    assert report(data)["unknown"] == 1
    data = snapshot()
    data["observations"] = []
    result = report(data)
    assert result["unknown"] == 1 and not result["final"]
    assert result["source_lag"]["unknown"] == 1


def test_preview_link_does_not_start_completed_deadline():
    data = snapshot()
    data["observations"][0]["first_seen_at"] = (ANCHOR - timedelta(days=7)).isoformat()
    result = report(data)
    assert result["matches"][0]["deadline"] == (ANCHOR + timedelta(hours=24)).isoformat()
    assert result["on_time"] == 1
    data["readiness"][0]["bronze_ready_at"] = (ANCHOR - timedelta(days=1)).isoformat()
    assert report(data)["unknown"] == 1


@pytest.mark.parametrize("cid,name,gender,section", [
    ("Big5", "Combined", "male", ""), ("850", "League", "male", ""),
    ("851", "League", "male", ""), ("852", "League", "male", ""),
    ("853", "League", "male", ""), ("999", "League", "female", ""),
    ("999", "U21 League", "male", ""), ("999", "League", "male", "Domestic Youth Leagues"),
    ("999", "Reserve League", "male", ""), ("999", "Premier League 2", "male", ""),
])
def test_population_excludes_all_nonadult_classes(cid, name, gender, section):
    assert not adult_competition({"competition_id": cid, "name": name, "gender": gender,
                                  "metadata": {"source_section": section}})


def test_missing_whole_schedule_cannot_disappear_or_pass():
    data = snapshot()
    extra = deepcopy(data["competitions"][0])
    extra.update(competition_id="82", name="Indian Super League")
    data["competitions"].append(extra)
    data["seasons"].append({**data["seasons"][0], "competition_id": "82"})
    result = report(data)
    assert result["adult_universe"] == 2 and result["active_current_scopes"] == 2
    assert result["scope_gaps"][0]["competition_id"] == "82"
    assert not result["final"]


def test_quarantined_adult_stays_a_population_blocker_until_proven_retired():
    data = snapshot()
    extra = deepcopy(data["competitions"][0])
    extra.update(competition_id="82", name="Indian Super League", crawl_state="quarantined")
    data["competitions"].append(extra)
    data["seasons"].append({**data["seasons"][0], "competition_id": "82"})
    result = report(data)
    assert result["scope_gaps"] and not result["milestone_day_pass"]
    extra.update(competition_id="68", name="USL First Division", crawl_state="skipped",
                 metadata={"current_scope_lifecycle": "discontinued", "current_scope_reason": "last_source_season_2009"})
    data["seasons"][-1]["competition_id"] = "68"
    assert not report(data)["scope_gaps"]


def test_unknown_fotmob_utc_remains_a_comparator_blocker():
    data = snapshot()
    data["fotmob_matches"].append({**data["fotmob_matches"][0], "utc_time": None, "match_id": "unknown"})
    assert report(data)["fotmob"] is None
    assert not report(data)["milestone_day_pass"]
    queries = []
    def pg(sql):
        return {"competitions": data["competitions"], "seasons": data["seasons"]} if "jsonb_build_object" in sql else []
    def trino(sql):
        queries.append(sql)
        return []
    collect_snapshot(pg, trino, day=DAY, as_of=AS_OF, mapping=MAP)
    fixture_query = next(q for q in queries if "fotmob_matches_current" in q)
    assert "try(from_iso8601_timestamp(utc_time)) IS NULL" in fixture_query


def test_deduplicate_source_matches_and_exclude_awards_postponed_and_youth():
    data = snapshot()
    data["schedules"].append(dict(data["schedules"][0]))
    data["schedules"].append({**data["schedules"][0], "match_url": None, "notes": "Awarded"})
    data["schedules"].append({**data["schedules"][0], "match_url": None, "score": ""})
    data["fotmob_matches"].append(dict(data["fotmob_matches"][0]))
    data["fotmob_matches"].append({**data["fotmob_matches"][0], "match_id": "other", "awarded": True})
    data["fotmob_matches"].append({**data["fotmob_matches"][0], "match_id": "other2", "postponed": "true"})
    assert report(data)["fotmob"] == report(data)["played"] == 1


def test_explicit_utc_day_overrides_local_fixture_date():
    data = snapshot()
    data["schedules"][0]["date"] = "2026-10-08"
    assert report(data)["played"] == 1
    data["observations"][0]["kickoff_at"] = None
    result = report(data)
    assert result["played"] == 0 and not result["final"]


def test_unknown_kickoff_blocks_date_confirmation_and_source_lag():
    data = snapshot()
    data["observations"][0]["kickoff_at"] = None
    result = report(data)
    assert result["played"] == 1 and not result["population_complete"]
    assert result["source_lag"]["samples"] == 0
    assert not result["final"]


def test_fotmob_missing_stale_or_extra_is_not_a_green_milestone():
    data = snapshot()
    data["fotmob_seasons"] = []
    result = report(data)
    assert result["fotmob"] is None and not result["milestone_day_pass"]
    data = snapshot()
    data["fotmob_seasons"][0]["fetched_at"] = (AS_OF - timedelta(hours=25)).isoformat()
    assert report(data)["fotmob"] is None
    data = snapshot()
    data["fotmob_matches"].append({**data["fotmob_matches"][0], "match_id": "extra"})
    result = report(data)
    assert result["fotmob"] == 2 and not result["milestone_day_pass"]


def test_referee_season_identity_is_scoped_and_phase_is_not_guessed():
    assert season_token("2026/27") == season_token("2026-2027")
    assert season_token("Apertura 2026") is None
    data = snapshot()
    data["seasons"][0]["season_id"] = "edition-2026"
    data["fotmob_seasons"][0]["season_key"] = "Apertura 2026"
    assert report(data)["fotmob_problems"][0]["reason"] == "unsupported_season_identity"
    data = snapshot()
    data["fotmob_matches"].append({**data["fotmob_matches"][0], "season_key": "2025/2026", "match_id": "wrong"})
    assert report(data)["fotmob"] == 1


def test_percentiles_and_three_consecutive_final_days():
    assert percentile95([1, 2, 3, 4]) == pytest.approx(3.85)
    assert percentile95([]) is None
    assert percentile95([5]) == 5
    days = [{"date": f"2026-10-{d:02d}", "final": True, "milestone_day_pass": True} for d in (4, 5, 6)]
    assert milestone_streak(days + [{"date": "2026-10-07", "final": False}]) == 3
    days[1]["final"] = False
    assert milestone_streak(days) == 1
    assert milestone_streak([days[0], days[2]]) == 1


def test_empty_and_failed_reads_are_explicit_not_successful_days():
    data = snapshot()
    data["schedules"] = []
    result = report(data)
    assert result["on_time_percent"] is None and not result["final"]
    def fail(_):
        raise RuntimeError("secret must not leak")
    result = collect_snapshot(fail, fail, day=DAY, as_of=AS_OF, mapping=MAP)
    assert result["errors"] and "secret" not in json.dumps(result)


def test_observation_reads_epoch_and_never_invents_timezone():
    rows = [{"match_url": "/en/matches/12345678/Test", "score": "2–1"}]
    html = '<table id="sched_all"><tr><td><span data-venue-time="17:00" data-venue-epoch="1791392400"></span><a href="/en/matches/12345678/Test">Report</a></td></tr></table>'
    result = schedule_observations(html, rows)
    assert result[0]["completed"] and result[0]["kickoff_at"].tzinfo
    result = schedule_observations(html.replace(' data-venue-epoch="1791392400"', ''), rows)
    assert result[0]["kickoff_at"] is None
    rows[0]["score"] = ""
    assert not schedule_observations(html, rows)[0]["completed"]


def test_report_contains_lag_and_no_unqualified_success_for_unknown():
    output = render_markdown(report())
    assert "95-й процентиль" in output and "UTC" in output and "МСК" in output
    mapping = json.loads((Path(__file__).resolve().parents[3] / 'configs/fbref/fotmob-report-map.json').read_text())
    assert len(mapping["competitions"]) == 112
    assert not {"850", "851", "852", "853", "Big5"} & set(mapping["competitions"])
