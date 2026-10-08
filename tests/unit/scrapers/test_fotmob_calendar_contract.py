"""Behavioral regressions for #1288; no source or storage connections."""

import sqlite3
import json
from dataclasses import replace
from pathlib import Path

import pytest

from scrapers.fotmob.catalog import CatalogShapeError
from scrapers.fotmob.domain import ScopeRef
from scrapers.fotmob.parsers import parse_season_bundle
from scrapers.fotmob.planner import RunMode
from scrapers.fotmob.repository import (
    FotMobRepository,
    LEGACY_PARSER_VERSION,
    ManifestStatus,
    PARSER_VERSION,
    TableRows,
)
from scrapers.fotmob.transport import FetchOutcome, canonicalize_target
from tests.unit.scrapers.test_fotmob_repository import _commit
from tests.unit.scrapers.test_fotmob_service import _league_payload, _service


def calendar(count=30):
    payload = _league_payload("2026")
    payload["details"]["id"] = 10618
    match = payload["fixtures"]["allMatches"][0]
    payload["fixtures"]["allMatches"] = [dict(match, id=i) for i in range(1, count + 1)]
    return payload


@pytest.mark.parametrize(
    "bad",
    [None, {}, {"hasOngoingMatch": False}, {"allMatches": {}}, {"allMatches": None}],
)
def test_malformed_calendar_is_not_an_empty_success(bad):
    payload = calendar()
    payload["fixtures"] = bad
    with pytest.raises(CatalogShapeError, match="calendar"):
        parse_season_bundle(payload, ScopeRef(10618, "2026"))


@pytest.mark.parametrize("location", ["fixtures", "nested", "matches", "overview"])
def test_explicit_empty_calendar_supported(location):
    payload = calendar(0)
    del payload["fixtures"]
    if location == "nested":
        payload["fixtures"] = {"data": {"allMatches": []}}
    elif location == "overview":
        payload["overview"] = {"leagueOverviewMatches": []}
    else:
        payload[location] = {"allMatches": []}
    assert parse_season_bundle(payload, ScopeRef(10618, "2026")).matches == ()


@pytest.mark.parametrize("mode", [RunMode.DAILY, RunMode.BACKFILL, RunMode.REPLAY])
@pytest.mark.parametrize("variant", ["empty", "missing", "prefetched", "304"])
def test_known_calendar_never_publishes_zero_replacement(mode, variant):
    good = calendar()
    bad = calendar(0)
    if variant == "missing":
        bad["fixtures"] = {"hasOngoingMatch": False}
        bad["tabs"] = []
    url = canonicalize_target("leagues", {"id": 10618, "season": "2026"}).canonical_url
    service, transport, repo = _service({url: [good, bad]})
    first, _ = service.sync_season(10618, "2026")
    assert first.succeeded == 1 and first.counts["matches"] == 30
    service.mode = mode
    kwargs = {}
    if variant in {"prefetched", "304"}:
        fetch = transport.fetch_json(url)
        if variant == "304":
            fetch = replace(fetch, outcome=FetchOutcome.NOT_MODIFIED, http_status=304)
        kwargs["prefetched"] = fetch
    result, bundle = service.sync_season(10618, "2026", **kwargs)
    assert result.succeeded == 0
    assert bundle is None
    assert repo.commits[-1].status == ManifestStatus.SCHEMA_DRIFT
    assert len(repo.tables["fotmob_matches"]) == 30


def test_empty_new_scope_is_valid_and_dependency_cannot_hide_known_matches():
    url = canonicalize_target("leagues", {"id": 10618, "season": "2026"}).canonical_url
    service, _, repo = _service({url: [calendar(0), calendar(), calendar(0)]})
    assert service.sync_season(10618, "2026")[0].succeeded == 1
    assert service.sync_season(10618, "2026")[0].succeeded == 1
    before = len(repo.commits)
    result, bundle = service.read_season_dependency(10618, "2026")
    assert result.succeeded == 0 and bundle is None
    assert len(repo.commits) == before


class CurrentSQL:
    """Execute production SELECT/window/join SQL, adapting only Trino DDL names."""

    def __init__(self):
        self.db = sqlite3.connect(":memory:")
        self.columns = {
            "fotmob_matches": [
                "competition_id",
                "source_season_key",
                "match_id",
                "_target_batch_id",
                "_observed_at",
                "_ingested_at",
            ],
            "fotmob_match_payloads": [
                "competition_id",
                "source_season_key",
                "match_id",
                "_target_batch_id",
                "_observed_at",
                "_ingested_at",
            ],
            "fotmob_ingest_manifest": [
                "batch_id",
                "target_key",
                "parser_version",
                "status",
                "target_type",
                "competition_id",
                "source_season_key",
                "entity_id",
                "completed_at",
            ],
        }
        self.sql = []
        for table, columns in self.columns.items():
            self.db.execute(f"CREATE TABLE {table} ({', '.join(columns)})")

    def _get_trino_manager(self):
        return self

    def table_exists(self, schema, table):
        return bool(
            self.db.execute(
                "SELECT 1 FROM sqlite_master WHERE name=?", (table,)
            ).fetchone()
        )

    def get_table_columns(self, schema, table):
        return self.columns[table]

    def _execute(self, sql):
        self.sql.append(sql)
        adapted = sql.replace("iceberg.bronze.", "")
        if "CREATE OR REPLACE VIEW" in adapted:
            view = adapted.split("VIEW", 1)[1].split()[0]
            self.db.execute(f"DROP VIEW IF EXISTS {view}")
            adapted = adapted.replace("CREATE OR REPLACE VIEW", "CREATE VIEW")
        return self.db.execute(adapted)

    def execute_query(self, sql):
        return self._execute(sql).fetchall()

    def manifest(
        self,
        batch,
        *,
        kind="league_season",
        season="2026",
        cid="10618",
        version=PARSER_VERSION,
        status="success",
        entity=None,
        time=None,
    ):
        self.db.execute(
            "INSERT INTO fotmob_ingest_manifest VALUES (?,?,?,?,?,?,?,?,?)",
            (batch, batch, version, status, kind, cid, season, entity, time or batch),
        )

    def rows(self, batch, ids, *, table="fotmob_matches", cid="10618", season="2026"):
        self.db.executemany(
            f"INSERT INTO {table} VALUES (?,?,?,?,?,?)",
            [(cid, season, str(mid), batch, batch, batch) for mid in ids],
        )

    def current(self, table="fotmob_matches"):
        return self.db.execute(
            f"SELECT competition_id, source_season_key, match_id FROM {table}_current ORDER BY 1,2,3"
        ).fetchall()


def test_current_recovers_last_good_through_zero_304_and_preserves_tombstones():
    db = CurrentSQL()
    db.manifest("01-good", version=LEGACY_PARSER_VERSION)
    db.rows("01-good", range(30))
    db.manifest("02-empty")
    db.manifest("03-304", status="not_modified")
    repo = FotMobRepository(writer=db)
    repo.ensure_current_views()
    assert len(db.current()) == 30
    db.manifest("04-gone", status="not_available")
    repo.ensure_current_views()
    assert db.current() == []
    db.manifest("05-empty")
    repo.ensure_current_views()
    assert db.current() == []
    db.manifest("06-back")
    db.rows("06-back", [1])
    repo.ensure_current_views()
    assert db.current() == [("10618", "2026", "1")]


def test_payload_membership_follows_calendar_with_independent_batch_ids():
    db = CurrentSQL()
    db.manifest("01-old")
    db.rows("01-old", [1, 2])
    for mid in (1, 2):
        db.manifest(f"02-card-{mid}", kind="match", entity=str(mid))
        db.rows(f"02-card-{mid}", [mid], table="fotmob_match_payloads")
    db.manifest("03-new")
    db.rows("03-new", [1])
    repo = FotMobRepository(writer=db)
    repo.ensure_current_views()
    assert (
        db.current("fotmob_match_payloads") == db.current() == [("10618", "2026", "1")]
    )
    # A match in a different scope must not admit the orphan card.
    db.manifest("04-other", season="2025")
    db.rows("04-other", [2], season="2025")
    repo.ensure_current_views()
    assert db.current("fotmob_match_payloads") == [("10618", "2026", "1")]


def test_calendar_batch_presence_handles_duplicate_and_null_physical_batch_keys():
    db = CurrentSQL()
    db.manifest("01-good")
    db.rows("01-good", [1, 1, 2])
    db.rows(None, [99])
    db.manifest("02-empty")
    repo = FotMobRepository(writer=db)
    repo.ensure_current_views()
    assert db.current() == [("10618", "2026", "1"), ("10618", "2026", "2")]
    db.manifest("03-removed", status="not_available")
    repo.ensure_current_views()
    assert db.current() == []


def test_known_match_lookup_uses_committed_physical_history_not_poisoned_current():
    db = CurrentSQL()
    db.manifest("01-good", version=LEGACY_PARSER_VERSION)
    db.rows("01-good", [1])
    db.manifest("02-empty")
    db.rows("uncommitted", [2], season="2025")
    repo = FotMobRepository(writer=db)
    assert repo.has_committed_matches(10618, "2026") is True
    assert repo.has_committed_matches(10618, "2025") is False
    assert repo.has_committed_matches(47, "2026") is False


def test_known_match_lookup_includes_pending_success_before_flush():
    db = CurrentSQL()
    repo = FotMobRepository(writer=db, batch_size=50)
    commit = _commit(
        target_type="league_season", competition_id="10618", source_season_key="2026"
    )
    repo.commit(
        commit,
        [
            TableRows(
                "fotmob_matches",
                [
                    {
                        "competition_id": "10618",
                        "source_season_key": "2026",
                        "match_id": "1",
                    }
                ],
                "matches",
            )
        ],
    )
    assert repo.has_committed_matches(10618, "2026") is True
    assert repo.has_committed_matches(10618, "2025") is False


def test_known_match_query_failure_is_retryable_without_publication(monkeypatch):
    url = canonicalize_target("leagues", {"id": 10618, "season": "2026"}).canonical_url
    service, _, repo = _service({url: calendar(0)})

    def unavailable(*args):
        raise OSError("storage offline")

    monkeypatch.setattr(repo, "has_committed_matches", unavailable)
    result, bundle = service.sync_season(10618, "2026")
    assert result.status == "retryable" and bundle is None
    assert repo.commits == []


def test_nonempty_calendar_does_not_query_history(monkeypatch):
    url = canonicalize_target("leagues", {"id": 10618, "season": "2026"}).canonical_url
    service, _, repo = _service({url: calendar()})

    def unexpected(*args):
        raise AssertionError("normal ingestion must not add a query")

    monkeypatch.setattr(repo, "has_committed_matches", unexpected)
    assert service.sync_season(10618, "2026")[0].succeeded == 1


def test_current_keeps_v2_priority_and_deduplicates_replayed_manifest():
    db = CurrentSQL()
    db.manifest("01-v2")
    db.rows("01-v2", [1])
    db.manifest("01-v2", status="not_modified", time="04-replay")
    db.manifest("02-v1", version=LEGACY_PARSER_VERSION)
    db.rows("02-v1", [2])
    db.rows("03-uncommitted", [3])
    FotMobRepository(writer=db).ensure_current_views()
    assert db.current() == [("10618", "2026", "1")]


def test_retained_10618_raw_versions_preserve_good_and_reject_bad():
    fixtures = Path(__file__).parents[2] / "fixtures/fotmob"
    good = json.loads((fixtures / "league_10618_2026_good.json").read_text())
    bad = json.loads((fixtures / "league_10618_2026_missing_calendar.json").read_text())
    url = canonicalize_target("leagues", {"id": 10618, "season": "2026"}).canonical_url
    service, _, repo = _service({url: [good, bad]})
    result, bundle = service.sync_season(10618, "2026")
    assert result.ok and result.counts["matches"] == 30
    batch = repo.commits[-1].batch_id
    result, bundle = service.sync_season(10618, "2026")
    assert not result.ok and result.succeeded == 0 and bundle is None
    assert repo.commits[-1].status == ManifestStatus.SCHEMA_DRIFT
    assert {row["_target_batch_id"] for row in repo.tables["fotmob_matches"]} == {batch}


def test_runner_does_not_close_scope_after_empty_known_calendar(monkeypatch):
    from tests.unit.scrapers.test_run_fotmob_scraper import (
        TestFotmobNativeRunner,
        _run_native_admitted,
    )

    mod = TestFotmobNativeRunner._module()
    monkeypatch.setenv(mod.WRITER_LOCK_ENV, "0")
    good = _league_payload()
    empty = _league_payload()
    empty["fixtures"]["allMatches"] = []
    exact = canonicalize_target("leagues", {"id": 47, "season": "2025/2026"})
    responses = {
        exact.canonical_url: good,
        canonicalize_target("allLeagues").canonical_url: {
            "countries": [
                {
                    "ccode": "ENG",
                    "name": "England",
                    "leagues": [{"id": 47, "name": "Premier League"}],
                }
            ]
        },
        canonicalize_target("leagues", {"id": 47}).canonical_url: empty,
    }
    service, _, repo = _service(responses)
    assert service.sync_season(47, "2025/2026")[0].succeeded == 1
    args = mod._argument_parser().parse_args(
        ["--mode", "daily", "--scope", "47=2025/2026", "--entities", "season"]
    )
    rc, report = _run_native_admitted(mod, args, service=service)
    assert rc == 1 and report["complete"] is False
    assert not any(c.target_type == "scope_completion" for c in repo.commits)
    assert any(c.status == ManifestStatus.SCHEMA_DRIFT for c in repo.commits)
