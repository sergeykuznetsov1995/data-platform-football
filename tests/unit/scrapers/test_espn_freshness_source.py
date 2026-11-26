"""Scheduled raw receipts -> authoritative source list -> real daily SQL and both consumers."""

import json
from datetime import date, datetime, timedelta, timezone

import pytest
import sqlglot

from scrapers.espn import criterion, urls
from scrapers.espn.measure_pace import read_freshness
from scrapers.espn.raw_store import EspnRawStore
from scrapers.espn.transport_contracts import canonicalize_target
from tests.unit.scrapers import test_espn_criterion as meter_tests
from tests.unit.scrapers.test_espn_criterion import DAY, _row
from tests.unit.scrapers.test_espn_wave import _day, _event

pytestmark = pytest.mark.unit
D = date.fromisoformat(DAY)
START = datetime.combine(D, datetime.min.time(), timezone.utc)
TARGET = "eng.1"
RUN = f"scheduled__{(START - timedelta(hours=6)).isoformat()}"


class Archive:
    def __init__(self, store, conn):
        self.store = store
        self.conn = conn
        self.captures = []
        self.waves = [("(wave)", "green")]

    def add(self, req, body, stamp):
        fp = canonicalize_target(req.url, req.params)
        record = self.store.store(fp, req.endpoint, body, fetched_at=stamp.isoformat())
        self.captures.append(
            (
                fp.url_fingerprint,
                stamp,
                RUN,
                "success",
                record.raw_uri,
                record.content_hash,
            )
        )

    def board(self, offset, *events, slug="all", at=None):
        day = D + timedelta(days=offset)
        raw = (
            _day(*events)
            if slug == "all"
            else json.dumps(
                {"leagues": [{"id": "700", "slug": TARGET}], "events": events}
            ).encode()
        )
        self.add(
            urls.league_scoreboard_day(slug, day), raw, at or START + timedelta(hours=6)
        )

    def core(self, *ids, page=1):
        body = {
            "count": len(ids),
            "pageIndex": page if ids else 0,
            "pageCount": page if ids else 0,
            "items": [
                {
                    "$ref": f"https://sports.core.api.espn.com/v2/sports/soccer/events/{n}"
                }
                for n in ids
            ],
        }
        self.add(
            urls.events_window(TARGET, D - timedelta(days=2), D),
            json.dumps(body).encode(),
            START,
        )

    def execute_query(self, sql):
        if "espn_wave_tournament_v1" in sql:
            assert RUN in sql
            return self.waves
        if "espn_request_journal_v1" in sql:
            assert "task_id='plan_wave'" in sql and "lane='live'" in sql
            return sorted(self.captures, key=lambda item: item[1])
        query = sqlglot.transpile(sql, read="trino", write="duckdb")[0]
        return self.conn.execute(query.replace("iceberg.bronze.", "bronze.")).fetchall()


meter = meter_tests.meter


@pytest.fixture
def archive(tmp_path, monkeypatch, meter):
    monkeypatch.setenv("ESPN_RAW_STORE_URI", tmp_path.as_uri())
    return Archive(EspnRawStore.from_env(), meter.connection)


def played(event_id, when="2026-10-10T15:00Z"):
    return _event(TARGET, event_id=event_id, when=when, status="STATUS_FULL_TIME")


def test_missing_match_is_named_by_report_and_acceptance(archive, meter):
    archive.board(-2)
    archive.board(-1, played(81), played(82))
    archive.core(81, 82)
    meter.load([_row(81)])
    day = criterion.read_day(archive, DAY, {TARGET})
    assert (day.ok, day.due, day.missing_ids, day.coverage_gaps) == (1, 2, (82,), ())
    packet = criterion.report_packet(day, 3)
    assert packet["state"] == "measured" and packet["streak"] == 0
    assert (
        "1/2" in packet["line"] and "50.0" in packet["line"] and "82" in packet["line"]
    )
    gate = read_freshness(archive, [TARGET], START + timedelta(days=1))
    assert (gate["ok"], gate["due"], gate["eligible"], gate["missing_ids"]) == (
        1,
        2,
        False,
        [82],
    )


@pytest.mark.parametrize(
    "latest_kickoff, expected_ok",
    [("2026-10-10T15:01Z", 1), ("2026-10-10T18:30Z", 0)],
)
def test_final_publication_against_corrected_kickoff(
    archive, meter, latest_kickoff, expected_ok
):
    archive.board(-2)
    archive.board(-1, played(81, latest_kickoff))
    archive.core(81)
    # Bronze originally scheduled 15:00 and published the final result at 18:00.
    meter.load([_row(81, published=timedelta(hours=3))])
    day = criterion.read_day(archive, DAY, {TARGET})
    assert (day.ok, day.due, day.state) == (expected_ok, 1, "measured")


@pytest.mark.parametrize(
    "damage",
    [
        "absent",
        "stale",
        "no_night",
        "red",
        "wrong_first_page",
        "unlocated_id",
        "corrupt",
    ],
)
def test_unknown_coverage_cannot_extend_or_bridge_a_streak(archive, meter, damage):
    archive.board(-1, played(81))
    archive.core(
        81,
        *([90] if damage == "unlocated_id" else []),
        page=2 if damage == "wrong_first_page" else 1,
    )
    if damage != "absent":
        archive.board(-2, at=START - timedelta(days=2) if damage == "stale" else None)
    if damage == "no_night":
        archive.waves.clear()
    if damage == "red":
        archive.waves.append((TARGET, "red"))
    if damage == "corrupt":
        fp, stamp, run, disp, uri, _ = archive.captures[-1]
        archive.captures[-1] = (fp, stamp, run, disp, uri, "0" * 64)
    meter.load([_row(81)])
    day = criterion.read_day(archive, DAY, {TARGET})
    assert day.coverage_gaps == (TARGET,) and day.pct is None and not day.eligible
    assert criterion.report_packet(day, 3)["streak"] == 0
    assert "✅" not in criterion.report_packet(day, 3)["line"]
    assert criterion.summarize_days([criterion.DayResult("2026-10-10", 1, 1), day]) == 0
    assert not read_freshness(archive, [TARGET], START + timedelta(days=1))["eligible"]


def test_confirmed_empty_day_is_neutral(archive):
    archive.board(-2)
    archive.board(-1)
    archive.core()
    day = criterion.read_day(archive, DAY, {TARGET})
    assert (day.ok, day.due, day.coverage_gaps) == (0, 0, ())
    assert criterion.report_packet(day, 3)["streak"] == 3
    assert criterion.report_packet(day, 3)["state"] == "neutral"
    assert not read_freshness(archive, [TARGET], START + timedelta(days=1))["eligible"]


def test_deadline_edges_and_core_topup_deduplicate_by_match(archive, meter):
    archive.board(-2, played(80, "2026-10-09T21:59Z"), played(81, "2026-10-09T22:00Z"))
    archive.board(-1, played(82, "2026-10-10T21:59Z"), played(83, "2026-10-10T22:00Z"))
    archive.board(-1, played(82, "2026-10-10T21:59Z"), played(84), slug=TARGET)
    archive.core(80, 81, 82, 83, 84, 85)
    meter.load(
        [_row(85, kickoff=datetime(2026, 10, 11, 13))]
    )  # core ID in a different deadline day
    day = criterion.read_day(archive, DAY, {TARGET})
    assert (day.due, day.missing_ids, day.coverage_gaps) == (3, (81, 82, 84), ())


@pytest.mark.parametrize("exempt,expected", [(set(), "unknown"), ({TARGET}, "neutral")])
def test_empty_scoreboards_require_positive_evidence_for_skipping_core(
    archive, monkeypatch, exempt, expected
):
    archive.board(-2)
    archive.board(-1)
    monkeypatch.setattr(
        "scrapers.espn.freshness_source._out_of_season", lambda run, day: exempt
    )
    day = criterion.read_day(archive, DAY, {TARGET})
    assert criterion.report_packet(day, 2)["state"] == expected


def test_truncated_scoreboard_needs_the_complete_league_topup(archive):
    other = [
        {"id": str(i), "uid": f"s:600~l:99999~e:{i}"}
        for i in range(1, urls.LIST_LIMIT + 1)
    ]
    archive.board(-2)
    archive.board(-1, *other)
    archive.core()
    assert criterion.read_day(archive, DAY, {TARGET}).coverage_gaps == (TARGET,)
    archive.board(-1, slug=TARGET)
    assert (
        criterion.report_packet(criterion.read_day(archive, DAY, {TARGET}), 2)["state"]
        == "neutral"
    )


def test_known_nonplayed_and_duplicates_stay_outside_denominator(archive, meter):
    postponed = _event(
        TARGET, event_id=81, when="2026-10-10T15:00Z", status="STATUS_POSTPONED"
    )
    archive.board(-2)
    archive.board(-1, postponed, played(82), played(83))
    archive.core(81, 82, 83)
    meter.load([_row(82, duplicate_of="espn:83"), _row(83)])
    day = criterion.read_day(archive, DAY, {TARGET})
    assert (day.due, day.ok, day.nonplayed, day.missing_ids, day.eligible) == (
        1,
        1,
        1,
        (),
        True,
    )
