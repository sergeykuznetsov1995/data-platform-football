from datetime import datetime, timedelta, timezone
import json
import time

import pytest

from dags.scripts import run_transfermarkt_achievements as runner
from scrapers.transfermarkt.achievement_observations import (
    AchievementRecipient,
    AchievementUniverse,
)
from scrapers.transfermarkt.achievement_state import AchievementState
from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
from scrapers.transfermarkt.raw_store import RawResponseStore

NOW = datetime(2026, 11, 20, 12, tzinfo=timezone.utc)
COACH_HTML = (
    b'<div class="box"><h2 class="content-box-headline">1x Cup</h2><table>'
    b'<tr><td>2022</td><td><a href="/club/startseite/verein/21">Club</a></td></tr>'
    b'</table></div>'
)


def body_for(recipient):
    if recipient.recipient_type == "coach":
        return COACH_HTML
    return json.dumps({
        "success": True,
        "data": {
            "aggregated": [{"id": 17, "receivedCount": 1, "name": "Cup"}],
            "achievements": [{
                "id": 17, "name": "Cup", "seasonId": 2022,
                "season": {"id": 2022, "display": "2022"},
                "details": {"playerId": 7},
            }],
        },
    }).encode()


class FakeClient:
    def __init__(self, store):
        self._raw_store = store
        self.calls = []
        self.bodies = {}

    def fetch(self, url, *, as_json, label, context):
        self.calls.append(url)
        body = self.bodies.get(url)
        if body is None:
            parts = url.split("/")
            kind = parts[3] if "tmapi." in url else "coach"
            identifier = parts[4] if kind != "coach" else parts[-1]
            slug = None if kind != "coach" else parts[3]
            body = body_for(AchievementRecipient(kind, identifier, slug))
        record = self._raw_store.store_attempt(
            url=url, body=body, status_code=200, headers={}, fetched_at=NOW.isoformat(),
            cycle_id=context.get("cycle_id", "run"), scope_id=context["scope_id"],
            endpoint=label, attempt=len(self.calls),
        )
        return FetchOutcome(
            status=FetchStatus.OK, value=body, status_code=200,
            raw_capture_id=record.capture_id, raw_body_hash=record.content_hash,
        )


class FakeScraper:
    def __init__(self, store):
        self._http_client = FakeClient(store)


def universe(version="v1"):
    return AchievementUniverse(version, (
        AchievementRecipient("player", 7),
        AchievementRecipient("club", 8),
        AchievementRecipient("coach", 9, "coach-nine"),
    ))


def fake_writer(scraper, extraction, preflight, committed_at):
    return {
        "verified": True, "receipt_readback": True,
        "source_capture_id": extraction.raw_capture_id,
        "receipt_id": "receipt-" + extraction.raw_capture_id[:8],
    }


def run(scraper, state, roster, *, at=NOW, limit=3, proof=None, deadline=None):
    return runner.run_achievement_portion(
        scraper, state, roster, run_id="run-" + at.isoformat(), now=at,
        max_recipients=limit, post_season_proof=proof,
        deadline_monotonic=deadline, writer=fake_writer, preflight={},
    )


@pytest.fixture(autouse=True)
def fake_dq(monkeypatch):
    monkeypatch.setattr(runner, "_dq", lambda *args: {"status": "GREEN"})


def test_chunked_capture_resumes_frozen_universe(tmp_path):
    store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    scraper = FakeScraper(store)
    with AchievementState(tmp_path / "state.sqlite3") as state:
        first = run(scraper, state, universe(), limit=2)
        assert first["processed"] == 2 and first["remaining"] == 1
        assert first["status"] == "in_progress"
        second = run(scraper, state, universe("changed-source"), limit=2)
        assert second["processed"] == 1 and second["remaining"] == 0
        assert second["status"] == "complete"
        assert second["assessment_manifest_id"] == first["assessment_manifest_id"]
        assert len(scraper._http_client.calls) == 3
        assert {call for call in scraper._http_client.calls} == {
            "https://tmapi.transfermarkt.technology/player/7/achievement",
            "https://tmapi.transfermarkt.technology/club/8/achievement",
            "https://www.transfermarkt.com/coach-nine/erfolge/trainer/9",
        }


def test_monthly_noop_then_next_assessment(tmp_path):
    scraper = FakeScraper(RawResponseStore.from_uri((tmp_path / "raw").as_uri()))
    with AchievementState(tmp_path / "state.sqlite3") as state:
        run(scraper, state, universe())
        calls = len(scraper._http_client.calls)
        later = run(scraper, state, universe(), at=NOW + timedelta(days=1))
        assert later["status"] == "no_op" and later["processed"] == 0
        assert len(scraper._http_client.calls) == calls
        monthly = run(scraper, state, universe("v2"), at=NOW + timedelta(days=31))
        assert monthly["status"] == "complete" and monthly["processed"] == 3
        assert monthly["assessment_manifest_id"] != later["assessment_manifest_id"]


def test_post_season_proof_forces_once_only_refresh(tmp_path):
    scraper = FakeScraper(RawResponseStore.from_uri((tmp_path / "raw").as_uri()))
    with AchievementState(tmp_path / "state.sqlite3") as state:
        run(scraper, state, universe(), proof="season-2025")
        refreshed = run(
            scraper, state, universe("v2"), at=NOW + timedelta(days=1), proof="season-2026",
        )
        assert refreshed["processed"] == 3 and refreshed["post_season_proof"] == "season-2026"
        repeated = run(
            scraper, state, universe("v2"), at=NOW + timedelta(days=2), proof="season-2026",
        )
        assert repeated["status"] == "no_op"


def test_error_is_unresolved_and_retried_with_same_assessment(tmp_path):
    scraper = FakeScraper(RawResponseStore.from_uri((tmp_path / "raw").as_uri()))
    url = "https://www.transfermarkt.com/coach-nine/erfolge/trainer/9"
    scraper._http_client.bodies[url] = b"<html>broken</html>"
    with AchievementState(tmp_path / "state.sqlite3") as state:
        first = run(scraper, state, universe())
        assert first["errors"] == 1 and first["status"] == "in_progress"
        assert first["remaining"] == 1
        del scraper._http_client.bodies[url]
        second = run(scraper, state, universe())
        assert second["errors"] == 0 and second["status"] == "complete"
        assert second["assessment_manifest_id"] == first["assessment_manifest_id"]


def test_deadline_preserves_remaining_work_without_http(tmp_path):
    scraper = FakeScraper(RawResponseStore.from_uri((tmp_path / "raw").as_uri()))
    with AchievementState(tmp_path / "state.sqlite3") as state:
        report = run(scraper, state, universe(), limit=2, deadline=time.monotonic() - 1)
        assert report["processed"] == 0 and report["remaining"] == 3
        assert scraper._http_client.calls == []


def test_post_season_query_requires_real_close_or_completed_edition():
    class Cursor:
        def __init__(self):
            self.calls = 0

        def execute(self, sql, params=()):
            self.calls += 1

        def fetchone(self):
            return None

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

    assert runner.post_season_proof(Connection(), today=NOW) is None


def test_writer_failure_stops_portion_and_preserves_pending_work(tmp_path):
    scraper = FakeScraper(RawResponseStore.from_uri((tmp_path / "raw").as_uri()))
    with AchievementState(tmp_path / "state.sqlite3") as state:
        roster = universe()
        with pytest.raises(RuntimeError, match="commit uncertain"):
            runner.run_achievement_portion(
                scraper, state, roster, run_id="run-write", now=NOW,
                max_recipients=3, writer=FakeWriterFailure(), preflight={},
            )
        assert len(state.due_recipients(roster.assessment_manifest_id, limit=3)) == 3
        assert len(scraper._http_client.calls) == 1


class FakeWriterFailure:
    def __call__(self, *args, **kwargs):
        raise RuntimeError("commit uncertain")
