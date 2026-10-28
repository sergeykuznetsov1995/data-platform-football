"""Offline optional HTML queue and publication regressions (#1439)."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd
import json
import pytest

from scrapers.understat.cache_state import CacheState, atomic_json, read_json
from scrapers.understat.client import UnderstatClient, UnderstatHTTPError
from scrapers.understat.enrichment import ENRICHMENT_CONTRACTS, run_enrichment
from scrapers.understat.manifest import ManifestStatus
from tests.unit.scrapers.test_understat_match_html import recorded


NOW = datetime(2026, 7, 1, tzinfo=timezone.utc)


class Repository:
    catalog = "iceberg"

    def __init__(self):
        self.schedule = []
        self.players = []
        self.writes = {}
        self.batch = "batch-1"
        self.valid = True
        self.status = ManifestStatus.COMPLETE
        self.fail_write = False

    def _execute(self, sql, params=()):
        if "understat_player_match_stats" in sql:
            return deepcopy(self.players)
        rows = self.schedule
        if params:
            rows = [r for r in rows if (r["league"], r["season"], r["_batch_id"]) == params]
        return deepcopy(rows)

    def latest_attempt(self, scope):
        return SimpleNamespace(batch_id=self.batch, status=self.status)

    def verify_physical_batch(self, attempt):
        return self.valid

    def _get_writer(self):
        return self

    def write_dataframe(self, frame, **kwargs):
        if self.fail_write:
            raise OSError("writer failed")
        table = kwargs["table"]
        from scrapers.base.iceberg_writer import IcebergWriter
        import pyarrow as pa

        writer = IcebergWriter.__new__(IcebergWriter)
        arrow = writer._pandas_to_arrow(writer._add_metadata_columns(frame, "understat"))
        contract = next(c for c in ENRICHMENT_CONTRACTS if c.table_name == table)
        assert set(arrow.column_names) == set(contract.required_columns) | {
            "_source", "_entity_type", "_ingested_at", "_batch_id"}
        for column in ("yellow_card_minutes", "red_card_minutes"):
            if column in frame:
                assert arrow.schema.field(column).type in (pa.string(), pa.large_string())
                from scrapers.base.trino_manager import TrinoTableManager
                manager = TrinoTableManager.__new__(TrinoTableManager)
                for value in frame[column]:
                    assert isinstance(json.loads(value), list)
                    assert manager._format_sql_value(value, "varchar").startswith("'[")
        from scrapers.base.trino_manager import TrinoTableManager
        types = TrinoTableManager.__new__(TrinoTableManager).arrow_schema_to_trino(arrow.schema)
        assert {c: types[c].lower() for c in contract.required_columns} == contract.column_types
        assert table in {c.table_name for c in ENRICHMENT_CONTRACTS}
        assert kwargs["merge_keys"]
        rows = self.writes.setdefault(table, {})
        for row in frame.to_dict("records"):
            key = tuple(row[k] for k in kwargs["merge_keys"])
            rows[key] = row

    def add_match(self):
        _, row, players, _ = recorded(31209)
        self.schedule = [{**row, "league": "ENG-Premier League", "season": "2526",
                          "source_season_id": 2025, "_batch_id": self.batch,
                          "date": (NOW + timedelta(hours=12)).isoformat()}]
        self.players = players[["team_id", "player_id", "team_side", "yellow_cards", "red_cards"]].to_dict("records")


class Session:
    def __init__(self):
        self.headers = {}
        self.calls = []
        self.status = 200
        self.html = recorded(31209)[0]
        self.error = None

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        assert kwargs["allow_redirects"] is False
        if self.error:
            raise self.error
        return SimpleNamespace(status_code=self.status, text=self.html)


def setup(tmp_path):
    repo, session = Repository(), Session()
    clock = [NOW]
    sleeps = []
    client = UnderstatClient(session=session, cache_dir=tmp_path, now=lambda: clock[0],
                             monotonic=lambda: 0, sleep=sleeps.append)
    CacheState(tmp_path).count_request(NOW.date())
    assert run_enrichment(client, repo, now=NOW)["status"] == "activated"
    return repo, session, client, clock, sleeps


def test_new_match_idempotence_rebind_and_arrays(tmp_path):
    repo, session, client, clock, sleeps = setup(tmp_path)
    repo.add_match()
    assert run_enrichment(client, repo, now=NOW)["published"] == 1
    run_enrichment(client, repo, now=NOW)
    assert len(session.calls) == 1
    match = next(iter(repo.writes["understat_match_enrichment"].values()))
    assert match["whoscored_game_id"] == 1983568 and match["is_valid"]
    assert match["parser_version"] == "understat-html-v2"
    cards = repo.writes["understat_player_match_enrichment"]
    assert any(json.loads(row["yellow_card_minutes"]) for row in cards.values())
    repo.batch = "batch-2"
    repo.schedule[0]["_batch_id"] = repo.batch
    run_enrichment(client, repo, now=NOW)
    assert len(session.calls) == 1
    assert next(iter(repo.writes["understat_match_enrichment"].values()))["source_batch_id"] == "batch-2"


def test_baseline_history_activation_and_missing_journal(tmp_path):
    repo, session = Repository(), Session()
    repo.add_match()
    client = UnderstatClient(session=session, cache_dir=tmp_path)
    run_enrichment(client, repo, now=NOW)
    run_enrichment(client, repo, now=NOW + timedelta(days=1))
    assert not session.calls
    (tmp_path / "html_enrichment/queue.json").unlink()
    assert run_enrichment(client, repo, now=NOW)["status"] == "waiting"
    assert not session.calls


@pytest.mark.parametrize("failure", ["timeout", "html", "cache", "writer"])
def test_failures_do_not_change_json_and_retry_tomorrow(tmp_path, monkeypatch, failure):
    import scrapers.understat.enrichment as module

    repo, session, client, clock, _ = setup(tmp_path)
    repo.add_match()
    baseline = deepcopy(repo.schedule), deepcopy(repo.players), repo.batch, repo.status
    if failure == "timeout":
        session.error = TimeoutError()
    elif failure == "html":
        session.html = "broken"
    elif failure == "writer":
        repo.fail_write = True
    else:
        original = module.atomic_json
        def fail_cache(path, payload):
            if path.name.startswith("match_"):
                raise OSError("cache failed")
            original(path, payload)
        monkeypatch.setattr(module, "atomic_json", fail_cache)
    assert run_enrichment(client, repo, now=NOW)["failed"] == 1
    run_enrichment(client, repo, now=NOW)
    assert len(session.calls) == 1
    assert baseline == (repo.schedule, repo.players, repo.batch, repo.status)
    session.error, session.html, repo.fail_write = None, recorded(31209)[0], False
    monkeypatch.undo()
    clock[0] += timedelta(days=1)
    CacheState(tmp_path).count_request(clock[0].date())
    assert run_enrichment(client, repo, now=clock[0])["published"] == 1
    assert len(session.calls) == 2


def test_three_failed_days_stop_retry(tmp_path):
    repo, session, client, clock, _ = setup(tmp_path)
    repo.add_match()
    session.status = 503
    for day in range(5):
        clock[0] = NOW + timedelta(days=day)
        CacheState(tmp_path).count_request(clock[0].date())
        run_enrichment(client, repo, now=clock[0])
    assert len(session.calls) == 3
    state = read_json(tmp_path / "html_enrichment/queue.json")
    assert next(iter(state["games"].values()))["failures"] == 3


def test_json_one_to_zero_invalidates_both_grains_and_defers(tmp_path):
    repo, session, client, clock, _ = setup(tmp_path)
    repo.add_match()
    run_enrichment(client, repo, now=NOW)
    for player in repo.players:
        if player["yellow_cards"]:
            player["yellow_cards"] = 0
    run_enrichment(client, repo, now=NOW)
    for rows in repo.writes.values():
        assert all(not row["is_valid"] for row in rows.values())
    assert len(session.calls) == 1
    clock[0] += timedelta(days=1)
    CacheState(tmp_path).count_request(clock[0].date())
    assert run_enrichment(client, repo, now=clock[0])["failed"] == 1
    assert len(session.calls) == 2
    assert all(not row["is_valid"] for rows in repo.writes.values() for row in rows.values())


@pytest.mark.parametrize("physical,status", [(False, ManifestStatus.COMPLETE), (True, ManifestStatus.IN_PROGRESS)])
def test_unconfirmed_json_never_gets_html(tmp_path, physical, status):
    repo, session, client, *_ = setup(tmp_path)
    repo.add_match()
    repo.valid, repo.status = physical, status
    run_enrichment(client, repo, now=NOW)
    assert not session.calls and not repo.writes


def test_budget_wait_is_not_an_attempt_and_queue_survives_july(tmp_path):
    repo, session, client, clock, _ = setup(tmp_path)
    repo.add_match()
    atomic_json(tmp_path / f"daily-http-{NOW.date()}.json", {"requests": 500, "rotated": []})
    run_enrichment(client, repo, now=NOW)
    assert not session.calls
    state = read_json(tmp_path / "html_enrichment/queue.json")
    assert next(iter(state["games"].values()))["failures"] == 0
    clock[0] += timedelta(days=1)
    CacheState(tmp_path).count_request(clock[0].date())
    assert run_enrichment(client, repo, now=clock[0])["published"] == 1


def test_client_has_no_bootstrap_retry_or_redirect_and_shares_pacing(tmp_path):
    repo, session, client, clock, sleeps = setup(tmp_path)
    client.get_match_html(29049)
    client.get_match_html(31209)
    assert sleeps == [2.0]
    session.status = 302
    with pytest.raises(UnderstatHTTPError):
        client.get_match_html(31210)
    assert len(session.calls) == 3
    assert client.request_count == 3 and client.retry_count == 0
    state = read_json(tmp_path / f"daily-http-{NOW.date()}.json")
    assert state["requests"] == 4 and state["html_requests"] == 3


@pytest.mark.parametrize("ledger", [None, {}, {"requests": -1}, {"requests": 500},
                                     {"requests": 100, "html_requests": 60}])
def test_unreliable_or_exhausted_budget_waits(tmp_path, ledger):
    path = tmp_path / f"daily-http-{NOW.date()}.json"
    if ledger is not None:
        atomic_json(path, ledger)
    assert not CacheState(tmp_path).claim_html(NOW.date())
    assert not CacheState(tmp_path).claim_html(NOW.date())


def test_atomic_budget_admission_and_rotation_reserve(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    cache = CacheState(tmp_path)
    atomic_json(tmp_path / f"daily-http-{NOW.date()}.json",
                {"requests": 499, "rotated": []})
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sum(pool.map(lambda _: cache.claim_html(NOW.date()), range(2))) == 1
    atomic_json(tmp_path / f"daily-http-{NOW.date()}.json",
                {"requests": 260, "rotated": [], "rotation_candidates": ["match_1.json", "match_2.json"]})
    assert cache.rotation(NOW.date(), mandatory=1) == {"match_1.json"}
    atomic_json(tmp_path / f"daily-http-{NOW.date()}.json", {"requests": 262, "rotated": []})
    assert not cache.claim_rotation("match_1.json", NOW.date())


def test_damaged_ledger_stays_unreliable_after_mandatory_json(tmp_path):
    (tmp_path / f"daily-http-{NOW.date()}.json").write_text("broken")
    cache = CacheState(tmp_path)
    cache.count_request(NOW.date())
    assert not cache.claim_html(NOW.date())


def test_error_of_one_scope_does_not_block_another(tmp_path, monkeypatch):
    repo, session, client, *_ = setup(tmp_path)
    repo.add_match()
    repo.schedule.append({**repo.schedule[0], "league": "AAA-failed"})
    original = repo.latest_attempt
    def latest(scope):
        if scope.league == "AAA-failed":
            raise OSError("one scope unavailable")
        return original(scope)
    monkeypatch.setattr(repo, "latest_attempt", latest)
    result = run_enrichment(client, repo, now=NOW)
    assert result["failed"] == 1
    assert result["published"] == 1
    assert len(session.calls) == 1


def test_late_discovered_history_is_not_enriched(tmp_path):
    repo, session, client, *_ = setup(tmp_path)
    repo.add_match()
    repo.schedule[0]["date"] = (NOW - timedelta(days=365)).isoformat()
    run_enrichment(client, repo, now=NOW)
    assert not session.calls


def test_same_match_is_claimed_once_per_utc_day(tmp_path):
    cache = CacheState(tmp_path)
    cache.count_request(NOW.date())
    assert cache.claim_html(NOW.date(), 29049)
    assert not cache.claim_html(NOW.date(), "29049")
    tomorrow = (NOW + timedelta(days=1)).date()
    cache.count_request(tomorrow)
    assert cache.claim_html(tomorrow, 29049)


def test_html_budget_uses_utc_date(tmp_path):
    repo, session, client, clock, _ = setup(tmp_path)
    clock[0] = datetime(2026, 7, 2, 1, tzinfo=timezone(timedelta(hours=2)))
    assert client.get_match_html(31209) == session.html
    assert len(session.calls) == 1
