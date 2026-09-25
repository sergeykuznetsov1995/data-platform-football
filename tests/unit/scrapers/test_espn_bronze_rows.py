"""Parser contracts -> ESPN bronze rows on recorded bodies (#1503)."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json

import pytest

from scrapers.espn.bronze_rows import (
    BatchStamp,
    MatchPayload,
    RawRef,
    batch_rows,
    event_rows,
    lineup_rows,
    match_row,
    team_stats_rows,
)
from scrapers.espn.parsers import SummaryDisposition, parse_summary
from tests.unit.scrapers.test_espn_probes import _bytes, _parse, _summary_context

RAW = RawRef(
    "s3://raw/blobs/sha256/ab/ab.json.gz",
    "ab" * 32,
    datetime(2026, 9, 25, 10, tzinfo=timezone.utc),
)
STAMP = BatchStamp("1" * 32, datetime(2026, 9, 25, 11))


def _rows(name: str):
    event, summary = _parse(name)
    kwargs = {"raw": RAW, "stamp": STAMP}
    return (
        event,
        summary,
        match_row(event, summary, **kwargs),
        lineup_rows(event, summary, **kwargs),
        team_stats_rows(event, summary, **kwargs),
        event_rows(event, summary, **kwargs),
    )


@pytest.mark.unit
def test_captured_summary_gives_full_match_and_children() -> None:
    event, summary, match, lineup, stats, events = _rows("summary_eng1_2020.json")

    assert (match["competition_slug"], match["season_year"], match["event_id"]) == (
        "eng.1",
        2020,
        578281,
    )
    assert match["disposition"] == "captured"
    assert json.loads(match["anomalies"]) == []
    assert (
        match["lineup_state"],
        match["team_stats_state"],
        match["events_state"],
    ) == (
        "captured",
        "captured",
        "captured",
    )
    assert match["deep_state"] == "pending"
    assert (match["home_score"], match["away_score"]) == (2, 0)
    assert match["home_score_h1"] + match["home_score_h2"] == 2
    assert {match["home_formation"], match["away_formation"]} == {"4-2-3-1", "4-3-3"}
    # The 2020-21 body has no gameInfo.officials: referee comes from 2010.
    assert match["referee"] is None
    assert _rows("summary_eng1_2010.json")[2]["referee"] == "Peter Walton"
    assert match["attendance"] == 10000
    assert match["first_fetched_at"] == datetime(2026, 9, 25, 10)
    assert match["rechecked_at"] is None
    # #1505: first publication = this batch; status read from the given body.
    assert match["first_published_at"] == STAMP.ingested_at
    assert match["status_checked_at"] == datetime(2026, 9, 25, 10)
    assert len(lineup) == 40
    assert not any(row["lineup_anomaly"] for row in lineup)
    assert {row["deep_stats_json"] for row in lineup} == {None}
    assert len(stats) == 2
    assert all(isinstance(row["total_shots"], float) for row in stats)
    assert len(events) == len(summary.events) == 12 + 86
    keys = {(row["kind"], row["event_key"]) for row in events}
    assert len(keys) == len(events)
    assert None not in {row["event_key"] for row in events}


@pytest.mark.unit
def test_world_cup_2010_has_statistics_but_no_players() -> None:
    _, _, match, lineup, stats, _ = _rows("summary_fifaworld_2010.json")

    assert match["lineup_state"] == "valid_empty"
    assert match["team_stats_state"] == "captured"
    assert lineup == []
    assert len(stats) == 2


@pytest.mark.unit
def test_honest_empty_keeps_match_facts_without_lineup_and_statistics() -> None:
    _, summary, match, lineup, stats, events = _rows("summary_gua1_2026_no_roster.json")

    assert match["disposition"] == "valid_empty"
    assert (match["home_score_h1"], match["home_score_h2"]) == (3, 0)
    assert (match["away_score_h1"], match["away_score_h2"]) == (0, 0)
    assert lineup == [] and stats == []
    # Key events are not part of the lineup/statistics emptiness.
    assert len(events) == len(summary.events)


@pytest.mark.unit
def test_lineup_without_statistics_keeps_venue_on_the_match() -> None:
    _, _, match, lineup, stats, _ = _rows("summary_uru1_2026_ten_starters.json")

    assert match["team_stats_state"] == "valid_empty"
    assert stats == []
    assert len(lineup) == 40
    assert match["venue_id"] == 10435


@pytest.mark.unit
def test_lineup_anomaly_flags_only_the_team_with_ten_starters() -> None:
    _, summary, match, lineup, _, _ = _rows("summary_jpn1_2026_ten_starters.json")

    assert summary.disposition is SummaryDisposition.LINEUP_ANOMALY
    assert match["disposition"] == "lineup_anomaly"
    assert json.loads(match["anomalies"]) == ["starters_not_11"]
    flagged = {row["team_id"] for row in lineup if row["lineup_anomaly"]}
    assert flagged == {3384}
    assert len(lineup) == 39


@pytest.mark.unit
def test_contradictory_flag_marks_the_team_of_that_player() -> None:
    _, _, _, lineup, _, _ = _rows("summary_arg2_2026_contradictory_flag.json")

    flagged = {row["team_id"] for row in lineup if row["lineup_anomaly"]}
    athlete_team = {row["athlete_id"]: row["team_id"] for row in lineup}
    assert flagged == {athlete_team[408183]}


@pytest.mark.unit
def test_source_malformed_gives_only_the_match_row_with_reason() -> None:
    raw = _bytes("summary_eng1_2020.json")
    competition, edition, event = _summary_context(json.loads(raw))
    broken = json.loads(raw)
    broken["rosters"] = "not a list"
    summary = parse_summary(
        json.dumps(broken).encode(),
        competition=competition,
        edition=edition,
        event=event,
    )
    assert summary.disposition is SummaryDisposition.SOURCE_MALFORMED
    kwargs = {"raw": RAW, "stamp": STAMP}

    match = match_row(event, summary, **kwargs)

    assert match["disposition"] == "source_malformed"
    assert match["reason"]
    assert match["lineup_state"] == match["team_stats_state"] == "malformed"
    assert match["events_state"] == "malformed"
    # Facts come from the schedule when the Summary is unreadable.
    assert (match["home_score"], match["away_score"]) == (2, 0)
    assert lineup_rows(event, summary, **kwargs) == []
    assert team_stats_rows(event, summary, **kwargs) == []
    assert event_rows(event, summary, **kwargs) == []


@pytest.mark.unit
def test_match_without_summary_is_pending_with_no_children() -> None:
    event, _ = _parse("summary_eng1_2020.json")
    kwargs = {"raw": RAW, "stamp": STAMP}

    match = match_row(event, None, **kwargs)

    assert match["disposition"] is None
    assert match["lineup_state"] == "pending"
    assert match["team_stats_state"] == match["events_state"] == "pending"
    assert match["first_fetched_at"] is None
    assert match["first_published_at"] is None
    assert lineup_rows(event, None, **kwargs) == []
    assert team_stats_rows(event, None, **kwargs) == []
    assert event_rows(event, None, **kwargs) == []


@pytest.mark.unit
def test_unplayed_match_has_no_score() -> None:
    event, _ = _parse("summary_eng1_2020.json")
    scheduled = replace(
        event,
        status="STATUS_SCHEDULED",
        terminal=False,
        played_final=False,
        summary_required=False,
        home_score=0,
        away_score=0,
        attendance_value=0,
    )

    match = match_row(scheduled, None, raw=RAW, stamp=STAMP)

    assert (match["home_score"], match["away_score"]) == (None, None)
    assert match["attendance"] is None


@pytest.mark.unit
def test_every_row_of_a_batch_shares_ingested_at_and_batch_id() -> None:
    payloads = []
    for name in (
        "summary_eng1_2020.json",
        "summary_jpn1_2026_ten_starters.json",
        "summary_gua1_2026_no_roster.json",
    ):
        event, summary = _parse(name)
        payloads.append(MatchPayload(event, summary, RAW))
    stamp = BatchStamp(
        "2" * 32, datetime(2026, 9, 25, 12, tzinfo=timezone(timedelta(hours=2)))
    )

    tables = batch_rows(payloads, stamp=stamp)

    stamps = {
        (row["_ingested_at"], row["_batch_id"], row["_source"])
        for rows in tables.values()
        for row in rows
    }
    assert stamps == {(datetime(2026, 9, 25, 10), "2" * 32, "espn")}
    assert len(tables["espn_match"]) == 3


@pytest.mark.unit
def test_summary_of_another_match_is_refused() -> None:
    event, summary = _parse("summary_eng1_2020.json")
    other, _ = _parse("summary_gua1_2026_no_roster.json")

    with pytest.raises(ValueError, match="paired with"):
        batch_rows([MatchPayload(other, summary, RAW)], stamp=STAMP)


@pytest.mark.unit
def test_presence_of_a_match_that_left_its_day_goes_to_disposition() -> None:
    # #1504: withdrawn/moved are presence values of a match without Summary.
    event, parsed = _parse("summary_eng1_2020.json")
    payload = MatchPayload(event, None, RAW, "withdrawn")

    rows = batch_rows([payload], stamp=STAMP)

    (match,) = rows["espn_match"]
    assert (match["disposition"], match["lineup_state"]) == ("withdrawn", "pending")
    with pytest.raises(ValueError, match="unknown presence"):
        MatchPayload(event, None, RAW, "vanished")
    with pytest.raises(ValueError, match="without Summary"):
        MatchPayload(event, parsed, RAW, "moved")
