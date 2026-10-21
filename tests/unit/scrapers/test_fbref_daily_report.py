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


def test_1325_page_age_denominator_boundaries_and_exclusions():
    from scrapers.fbref.daily_report import page_age_line
    data = snapshot()
    base = {"page_kind": "season", "canonical_url": "https://fbref.com/en/comps/9/x",
            "state": "queued", "refresh_policy": "daily", "current_ownership": True,
            "scopes": [{"competition_id": "9", "season_id": "2026-2027"}]}
    data["page_ages"] = [
        dict(base, target_id="never", last_fetched_at=None),
        dict(base, target_id="edge", last_fetched_at=(AS_OF-timedelta(days=7)).isoformat()),
        dict(base, target_id="fresh", last_fetched_at=(AS_OF-timedelta(days=7, seconds=-1)).isoformat()),
        dict(base, target_id="future", last_fetched_at=(AS_OF+timedelta(seconds=1)).isoformat()),
        dict(base, target_id="dead", state="dead", last_fetched_at=None),
        dict(base, target_id="quarantine", state="quarantined", last_fetched_at=None),
        dict(base, target_id="history", refresh_policy="historical_once", last_fetched_at=None),
        dict(base, target_id="outside", current_ownership=False, last_fetched_at=None),
        dict(base, target_id="roster", page_kind="squad", last_fetched_at=(AS_OF-timedelta(days=27)).isoformat()),
        dict(base, target_id="logs", page_kind="squad", canonical_url="https://fbref.com/en/squads/abc/matchlogs/x", last_fetched_at=None),
    ]
    data["page_ages"].append(dict(data["page_ages"][2]))
    result = report(data)
    ages = result["page_ages"]
    assert ages["by_kind"]["season"]["total"] == 4
    assert ages["by_kind"]["season"]["fresh"] == 1
    assert ages["by_kind"]["season"]["never"] == 1
    assert ages["by_kind"]["season"]["aged"] == 1
    assert ages["by_kind"]["squad"]["fresh"] == 1
    assert ages["total"] == 5 and ages["fresh_percent"] == 40
    assert ages["excluded"] == {"dead": 1, "quarantined": 1, "history": 1,
                                "outside_current_adult_scope": 1, "non_roster": 1}
    line = page_age_line(result)
    assert "Возраст по видам страниц: " in line
    assert "season_stats: N=0" in line and "н/д" in line
    assert "fresh28=1" in line and "median/p95/max=" in line


def test_1325_reader_keeps_aliases_and_provenance_in_page_age_slice():
    from scrapers.fbref.daily_report_reader import PAGE_AGES_SQL
    queries = []
    data = snapshot()
    def pg(sql):
        queries.append(sql)
        if "jsonb_build_object(\n 'competitions'" in sql:
            return {k: data[k] for k in ("competitions", "seasons")}
        return []
    result = collect_snapshot(pg, lambda _: [], day=DAY, as_of=AS_OF, mapping=MAP)
    assert result["page_ages"] == []
    sql = next(q for q in queries if "current_ownership" in q)
    assert "season_alias" in sql and "frontier_provenance" in sql
    assert "jsonb_agg(DISTINCT" in sql and "f.last_fetched_at" in sql
    assert "refresh_policy = 'weekly'" not in PAGE_AGES_SQL


def test_1325_standings_proof_uses_processed_season_current_content():
    data = snapshot()
    base = {"page_kind": "season", "canonical_url": "https://fbref.com/en/comps/9/x",
            "state": "fetched", "refresh_policy": "weekly", "current_ownership": True,
            "scopes": [{"competition_id": "9", "season_id": "2026-2027"}],
            "last_fetched_at": AS_OF.isoformat(), "last_content_hash": "current", "processed": True}
    data["page_ages"] = [dict(base, target_id="season"), dict(base, target_id="unprocessed", processed=False),
                         dict(base, target_id="old-content"), dict(base, target_id="stats", page_kind="season_stats")]
    data["standings_inventory"] = [{"target_id": "season", "content_hash": "current"},
                                  {"target_id": "unprocessed", "content_hash": "current"},
                                  {"target_id": "old-content", "content_hash": "old"},
                                  {"target_id": "stats", "content_hash": "current"}]
    assert report(data)["page_ages"]["standings_confirmed_seasons"] == 1
    # Not every competition format publishes standings; absence stays evidence, not a failure.
    data["standings_inventory"] = []
    assert report(data)["page_ages"]["standings_confirmed_seasons"] == 0


def test_1325_optional_page_age_read_failure_does_not_break_match_milestone():
    data = snapshot()
    def pg(sql):
        if "jsonb_build_object(\n 'competitions'" in sql:
            return {k: data[k] for k in ("competitions", "seasons", "schedule_health")}
        if "AS current_ownership" in sql:
            raise RuntimeError("private credentials")
        if "jsonb_object_agg(reason,n)" in sql:
            return {}
        return []
    collected = collect_snapshot(pg, lambda _: [], day=DAY, as_of=AS_OF, mapping=MAP)
    assert collected["errors"] == []
    assert collected["page_age_errors"] == [{"source": "fbref_page_ages", "reason": "read_failed"}]
    assert "private" not in json.dumps(collected)


RAW_1792 = Path(__file__).resolve().parents[2] / "fixtures/fbref/report_observations"


def parsed_1792(cid, html=None):
    from scrapers.fbref.discovery import CalendarType, SeasonRef, parse_schedule_html
    html = html if html is not None else (RAW_1792 / f"schedule-{cid}.html").read_text()
    season = SeasonRef(str(cid), "2026-2027", "2026-2027", CalendarType.SPLIT_YEAR,
                       f"https://fbref.com/en/comps/{cid}/2026-2027/x")
    parsed = parse_schedule_html(html, season)
    assert not parsed.has_errors
    return html, list(parsed.datasets["schedule_rows"].records)


@pytest.mark.parametrize("commented", [False, True])
def test_1792_raw_date_link_before_report_does_not_hide_epoch(commented):
    html, rows = parsed_1792("15")
    if commented:
        html = "<!--" + html + "-->"
    observed = schedule_observations(html, rows)
    assert len(observed) == 1
    assert observed[0]["match_id"] == "27d4bb29"
    assert observed[0]["kickoff_at"] == datetime(2026, 8, 15, 11, 30, tzinfo=timezone.utc)
    assert observed[0]["completed"]


def test_1792_raw_played_saudi_match_has_confirmed_utc():
    html, rows = parsed_1792("70")
    assert schedule_observations(html, rows)[0]["kickoff_at"] == datetime(2026, 10, 20, 15, tzinfo=timezone.utc)


@pytest.mark.parametrize("cid,count", [("15", 1), ("16", 1)])
def test_1792_raw_oct20_has_no_score_or_match_report(cid, count):
    html, rows = parsed_1792(cid)
    future = [r for r in rows if r["date"] == "2026-10-20"]
    assert len(future) == count
    assert all(not r["score"] and not r["match_url"] for r in future)
    assert "Head-to-Head" in html


def test_1792_conflicting_epochs_are_unconfirmed_and_offset_crosses_utc_day():
    html, rows = parsed_1792("70")
    duplicate = html.replace('1792508400', '1792594800')
    assert schedule_observations(html + duplicate, rows)[0]["kickoff_at"] is None
    html = html.replace('data-venue-epoch="1792508400"', '').replace(
        'data-venue-time="18:00"', 'data-venue-time="2026-10-20T00:30:00+03:00"')
    assert schedule_observations(html, rows)[0]["kickoff_at"].astimezone(timezone.utc).date() == date(2026, 10, 19)
    html = html.replace('2026-10-20T00:30:00+03:00', '18:00')
    assert schedule_observations(html, rows)[0]["kickoff_at"] is None


@pytest.mark.parametrize("age,expected", [(24, 1), (24 + 1/3600, None), (-1, None)])
def test_1792_fotmob_validation_proves_freshness_without_rewriting_body_time(age, expected):
    data = snapshot()
    data["fotmob_seasons"][0].update(
        fetched_at=(AS_OF-timedelta(days=10)).isoformat(),
        validated_at=(AS_OF-timedelta(hours=age)).isoformat())
    result = report(data)
    assert result["fotmob"] == expected
    assert result["milestone_day_pass"] is (expected == 1)
    if age > 24:
        problem = result["fotmob_problems"][0]
        assert problem["reason"] == "stale_fotmob_season"
        assert problem["age_hours"] == pytest.approx(age)
        assert problem["last_confirmed_at"]


def test_1792_fotmob_stale_replay_cannot_confirm_freshness():
    data = snapshot()
    data["fotmob_seasons"][0]["stale"] = True
    assert report(data)["fotmob"] is None


def test_1792_reader_uses_source_validation_and_guards_cutoff():
    queries = []
    data = snapshot()
    def pg(sql):
        return {k: data[k] for k in ("competitions", "seasons")} if "'competitions'" in sql else []
    def trino(sql):
        queries.append(sql)
        return []
    collect_snapshot(pg, trino, day=DAY, as_of=AS_OF, mapping=MAP)
    query = next(q for q in queries if "fotmob_ingest_manifest" in q)
    assert "$.source_observed_at" in query and "max(validated_at)" in query
    assert "validated_at >= fetched_at" in query and "AND validated_at <=" in query
    assert "AND completed_at <=" in query and "NOT coalesce(stale,false)" in query
    assert "completed_at)" not in query  # ingestion time alone never proves freshness


def test_1792_reader_phase_catalog_uses_source_validation_after_304():
    queries = []
    data = snapshot()
    def pg(sql):
        return {k: data[k] for k in ("competitions", "seasons")} if "'competitions'" in sql else []
    def trino(sql):
        queries.append(sql)
        return []
    collect_snapshot(pg, trino, day=DAY, as_of=AS_OF, mapping=MAP)
    query = next(q for q in queries if "target_type='competition_seasons'" in q)
    assert "$.source_observed_at" in query
    assert "with_timezone(m.validated_at, 'UTC')) AS validated_at" in query
    assert "validated_at >= fetched_at" in query and "AND validated_at <=" in query
    assert "AND completed_at <=" in query and "NOT coalesce(m.stale,false)" in query


@pytest.mark.parametrize("cid,sid,fid,phase", [
    ("31", "2026-2027", "230", "2026/2027 - Apertura"),
    ("41", "2026", "274", "2026 - Clausura"),
])
def test_1792_exact_phase_mapping_counts_and_deduplicates(cid, sid, fid, phase):
    data = snapshot()
    data["competitions"][0]["competition_id"] = cid
    for kind in ("seasons", "schedule_health", "observations", "readiness"):
        data[kind][0].update(competition_id=cid, season_id=sid)
    data["schedules"][0].update(source_competition_id=cid, source_season_id=sid)
    data["fotmob_seasons"][0].update(competition_id=fid, season_key=phase)
    data["fotmob_matches"][0].update(competition_id=fid, season_key=phase)
    data["fotmob_matches"].append(dict(data["fotmob_matches"][0]))
    mapping = json.loads((Path(__file__).resolve().parents[3]/'configs/fbref/fotmob-report-map.json').read_text())["competitions"]
    keys = mapping[cid]['season_keys'][sid][fid]
    data['fotmob_seasons'] = [dict(data['fotmob_seasons'][0], season_key=k) for k in keys]
    data['fotmob_phase_catalog'] = deepcopy(data['fotmob_seasons'])
    result = build_daily_report(data, day=DAY, as_of=AS_OF, mapping=mapping)
    assert result["fotmob"] == 1 and result["milestone_day_pass"]
    data["fotmob_seasons"][0]["season_key"] = "2030 - Clausura"
    assert build_daily_report(data, day=DAY, as_of=AS_OF, mapping=mapping)["fotmob"] is None
    assert season_token(phase) is None


@pytest.mark.parametrize("cid,sid", [("257", "2024"), ("665", "2023"), ("666", "2017"), ("693", "2019-2020")])
def test_1792_old_latest_seasons_without_proof_remain_blockers(cid, sid):
    data = snapshot()
    data["competitions"].append({**data["competitions"][0], "competition_id": cid})
    data["seasons"].append({**data["seasons"][0], "competition_id": cid, "season_id": sid})
    data["schedule_health"].append({"competition_id": cid, "season_id": sid, "fetched": True})
    result = report(data)
    assert not result["milestone_day_pass"]
    assert result["day_scope_exclusions"] == []
    assert any(p["competition_id"] == cid and p["reason"] == "unmapped_fotmob_competition"
               for p in result["fotmob_problems"])


def test_1792_summary_names_date_deadline_comparator_blockers_even_with_unknown_zero():
    from scrapers.fbref.daily_report import summary_line
    data = snapshot()
    data["observations"][0]["kickoff_at"] = None
    data["readiness"][0]["bronze_ready_at"] = ANCHOR.isoformat()
    data["fotmob_seasons"] = []
    result = report(data, cutoff=ANCHOR+timedelta(hours=12))
    assert result["unknown"] == result["pending"] == 0
    line = summary_line(result)
    assert line.startswith("FBref: матчей ") and "\n" not in line
    assert "UTC не подтверждено 1" in line and "дедлайн не истёк 1" in line
    assert "missing_fotmob_season" in line and "известно частично 0" in line
    assert not result["final"] and not result["milestone_day_pass"]


def test_1792_registry_lag_is_visible_even_with_fresh_old_edition():
    from scrapers.fbref.daily_report import summary_line
    data = snapshot()
    data['seasons'][0]['season_id'] = '2023'
    data['schedule_health'][0]['season_id'] = '2023'
    data['schedules'] = data['observations'] = []
    data['fotmob_matches'] = []
    data['fotmob_seasons'] = [
        dict(data['fotmob_seasons'][0], season_key='2023'),
        dict(data['fotmob_seasons'][0], season_key='2027')]
    result = report(data)
    assert result['day_scope_exclusions'] == []
    assert result['fotmob'] is None and not result['milestone_day_pass']
    assert 'fbref_registry_behind_fotmob' in summary_line(result)


@pytest.mark.parametrize('evidence', ['fotmob_seasons', 'fotmob_matches'])
@pytest.mark.parametrize('phase', ['2026/2027 - Clausura', '2026/2027 Clausura', 'New phase'])
def test_1792_new_phase_blocks_false_zero_even_with_fresh_apertura(evidence, phase):
    data = snapshot()
    data['schedules'] = data['observations'] = []
    data['fotmob_seasons'][0]['season_key'] = '2026/2027 - Apertura'
    data['fotmob_matches'] = []
    if evidence == 'fotmob_seasons':
        data[evidence].append(dict(data['fotmob_seasons'][0], season_key=phase))
    else:
        data[evidence].append(dict(snapshot()['fotmob_matches'][0], season_key=phase))
    mapping = {'9': {'fotmob_ids': ['47'], 'season_keys': {
        '2026-2027': {'47': ['2026/2027 - Apertura']}}}}
    result = build_daily_report(data, day=DAY, as_of=AS_OF, mapping=mapping)
    assert result['fotmob'] is None
    assert any(p['reason'] == 'unknown_fotmob_season_phase' for p in result['fotmob_problems'])
    assert not result['milestone_day_pass']


@pytest.mark.parametrize('confirmation', ['missing', 'stale', 'fresh'])
def test_1792_every_mapped_phase_needs_its_own_fresh_evidence(confirmation):
    data = snapshot()
    data['fotmob_seasons'][0]['season_key'] = '2026/2027 - Apertura'
    data['fotmob_matches'][0]['season_key'] = '2026/2027 - Clausura'
    if confirmation != 'missing':
        data['fotmob_seasons'].append(dict(data['fotmob_seasons'][0],
            season_key='2026/2027 - Clausura',
            fetched_at=(AS_OF - timedelta(hours=25 if confirmation == 'stale' else 1)).isoformat()))
    data['fotmob_phase_catalog'] = [dict(data['fotmob_seasons'][0], season_key=k)
        for k in ['2026/2027 - Apertura', '2026/2027 - Clausura']]
    mapping = {'9': {'fotmob_ids': ['47'], 'season_keys': {
        '2026-2027': {'47': ['2026/2027 - Apertura', '2026/2027 - Clausura']}}}}
    result = build_daily_report(data, day=DAY, as_of=AS_OF, mapping=mapping)
    assert result['milestone_day_pass'] is (confirmation == 'fresh')
    assert result['fotmob'] == (1 if confirmation == 'fresh' else None)


@pytest.mark.parametrize('catalog_state', ['missing', 'stale', 'new_phase_unloaded', 'fresh'])
def test_1792_round3_phase_catalog_prevents_unseen_phase_false_zero(catalog_state):
    data = snapshot()
    data['schedules'] = data['observations'] = data['fotmob_matches'] = []
    data['fotmob_seasons'][0]['season_key'] = '2026/2027 - Apertura'
    mapping = {'9': {'fotmob_ids': ['47'], 'season_keys': {
        '2026-2027': {'47': ['2026/2027 - Apertura']}}}}
    if catalog_state != 'missing':
        data['fotmob_phase_catalog'] = [dict(data['fotmob_seasons'][0],
            fetched_at=(AS_OF - timedelta(hours=25 if catalog_state == 'stale' else 1)).isoformat())]
        if catalog_state == 'new_phase_unloaded':
            data['fotmob_phase_catalog'].append(dict(data['fotmob_phase_catalog'][0],
                season_key='2026/2027 - Clausura'))
    result = build_daily_report(data, day=DAY, as_of=AS_OF, mapping=mapping)
    assert result['fotmob'] == (0 if catalog_state == 'fresh' else None)
    assert not result['milestone_day_pass']  # Empty days never count.
    assert bool(result['fotmob_problems']) is (catalog_state != 'fresh')


def test_1792_round3_colombia_map_covers_both_snapshot_phases():
    mapping = json.loads(Path('configs/fbref/fotmob-report-map.json').read_text())['competitions']
    assert set(mapping['41']['season_keys']['2026']['274']) == {'2026 - Apertura', '2026 - Clausura'}


@pytest.mark.parametrize('cid', ['257', '665', '666', '693'])
@pytest.mark.parametrize('failure', ['none', 'utc', 'schedule', 'readiness', 'deadline'])
def test_1792_round3_absent_reference_keeps_all_fbref_guards(cid, failure):
    from scrapers.fbref.daily_report import summary_line
    data = snapshot()
    # Keep the ordinary mapped league as well, proving mixed coverage comparison.
    for name in ['competitions', 'seasons', 'schedule_health', 'schedules', 'observations', 'readiness']:
        extra = dict(data[name][0])
        key = 'source_competition_id' if name == 'schedules' else 'competition_id'
        extra[key] = cid
        if failure == 'utc' and name == 'observations':
            extra['kickoff_at'] = None
        if failure == 'schedule' and name == 'schedule_health':
            extra['fetched'] = False
        if failure == 'readiness' and name == 'readiness':
            extra['bronze_ready_at'] = None
        if failure == 'deadline' and name == 'observations':
            extra['first_completed_seen_at'] = (AS_OF - timedelta(hours=1)).isoformat()
        data[name].append(extra)
    mapping = {**MAP, cid: json.loads(Path('configs/fbref/fotmob-report-map.json').read_text())['competitions'][cid]}
    if failure == 'deadline':
        data['observations'][-1]['first_completed_seen_at'] = '2026-10-20T23:00:00Z'
    data['fotmob_seasons'][0]['fetched_at'] = '2026-10-20T23:00:00Z'
    result = build_daily_report(data, day=DAY, as_of=datetime(2026, 10, 21, tzinfo=timezone.utc), mapping=mapping)
    assert result['played'] == 2
    assert result['without_reference'] == [cid]
    assert result['fotmob'] == 1
    assert result['milestone_day_pass'] is (failure == 'none')
    assert 'без эталона: 1' in summary_line(result)
    assert result['competitions'][0 if cid < '9' else 1]['fotmob'] is None
    mapping[cid] = {'fotmob_ids': []}
    blocked = build_daily_report(data, day=DAY, as_of=datetime(2026, 10, 21, tzinfo=timezone.utc), mapping=mapping)
    assert not blocked['milestone_day_pass']
    assert any(p['reason'] == 'unmapped_fotmob_competition' for p in blocked['fotmob_problems'])


@pytest.mark.parametrize('attributes', [
    '<span data-venue-epoch="1791392400"></span><span data-venue-epoch="1791478800"></span>',
    '<span data-venue-epoch="1791392400" data-venue-time="2026-10-08T17:00:00Z"></span>',
    '<span data-venue-time="2026-10-07T17:00:00Z"></span><span data-venue-time="2026-10-08T17:00:00Z"></span>',
])
def test_1792_round3_conflicting_timestamps_inside_row_are_unconfirmed(attributes):
    html = '<table id="sched_all"><tr><td>' + attributes + '<a href="/en/matches/12345678/Test">Report</a></td></tr></table>'
    rows = [{'match_url': '/en/matches/12345678/Test', 'score': '2–1'}]
    assert schedule_observations(html, rows)[0]['kickoff_at'] is None


@pytest.mark.parametrize('fetched,validated', [
    (AS_OF + timedelta(hours=1), AS_OF - timedelta(hours=1)),
    (AS_OF, AS_OF - timedelta(hours=1)),
    (None, AS_OF - timedelta(hours=1)),
    (AS_OF - timedelta(hours=2), AS_OF + timedelta(hours=1)),
])
def test_1792_round3_saved_snapshot_rejects_invalid_confirmation_order(fetched, validated):
    data = snapshot()
    data['fotmob_seasons'][0].update(fetched_at=fetched.isoformat() if fetched else None,
                                   validated_at=validated.isoformat())
    result = report(data)
    assert result['fotmob'] is None and not result['milestone_day_pass']
