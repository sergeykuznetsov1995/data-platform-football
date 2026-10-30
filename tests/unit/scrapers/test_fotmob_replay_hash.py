"""Selected observations survive pointer movement and delayed publication (#1304)."""
import gzip
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from scrapers.fotmob.planner import RunMode
from scrapers.fotmob.raw_store import FotMobRawStore
from scrapers.fotmob.repository import (
    CURRENT_VIEW_SPECS, PARSER_VERSION, REPLACE_TARGET_CURRENT_TABLES, ManifestStatus,
    FotMobRepository, MemoryFotMobRepository, TargetCommit,
)
from scrapers.fotmob.service import FotMobIngestService, _validated_at
from scrapers.fotmob.transport import canonicalize_target
from tests.unit.scrapers.test_fotmob_service import _league_payload
from tests.unit.scrapers.test_fotmob_transport import FakeResponse, _transport

OLD = datetime(2026, 7, 11, 10, tzinfo=timezone.utc)


def _selected(store, target, payload, *, validated=OLD):
    record = store.store(target, json.dumps(payload).encode(), fetched_at=OLD.isoformat(),
                         validated_at=validated.isoformat())
    commit = TargetCommit(
        run_id="accepted", target_type="player", entity_id="10",
        target_key=target.target_key, status=ManifestStatus.SUCCESS,
        content_hash=record.content_hash, raw_uri=record.raw_uri,
        fetched_at=OLD, completed_at=OLD,
        capabilities={"canonical_url": target.canonical_url,
                      "source_observed_at": validated.isoformat()},
    )
    return record, commit


@pytest.mark.parametrize("stale", [False, True])
def test_selected_player_replay_and_alias_ignore_moved_pointer(tmp_path, stale):
    store = FotMobRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target("https://www.fotmob.com/_next/data/old/players/10.json")
    record, commit = _selected(store, target, {"pageProps": {"data": {"id": 10, "name": "A"}}})
    store.store(target, b'{"pageProps":{"data":{"id":10,"name":"B"}}}')
    transport, session = _transport([], raw_store=store)
    repository = MemoryFotMobRepository()
    repository.record(replace(commit, stale=stale))
    service = FotMobIngestService(transport=transport, repository=repository, mode=RunMode.REPLAY)
    result = service.sync_player_snapshots([10], repair_missing_snapshot=True)
    assert result.ok, result.errors
    assert repository.tables["fotmob_player_snapshots"][0]["name"] == "A"
    assert repository.commits[-1].content_hash == record.content_hash
    assert repository.commits[-1].stale == stale
    assert _validated_at(repository.commits[-1].manifest_row()) == (None if stale else OLD.replace(tzinfo=None))
    assert not session.calls

    alias = canonicalize_target("https://www.fotmob.com/_next/data/alias/players/10.json")
    fetch = transport.replay_target_key(target.target_key, observation=commit.manifest_row())
    transport.alias_cached_json(target.canonical_url, alias.canonical_url, observation=fetch)
    body, aliased = store.load(alias)
    assert json.loads(body)["pageProps"]["data"]["name"] == "A"
    assert aliased.validated_at == OLD.isoformat()


@pytest.mark.parametrize("damage", ["missing", "gzip", "hash", "json", "uri"])
def test_selected_blob_fails_closed_without_pointer_fallback(tmp_path, damage):
    store = FotMobRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target("teams", {"id": 1})
    payload = {"details": {"id": 1}}
    record, commit = _selected(store, target, payload)
    store.store(target, b'{"details":{"id":1,"name":"B"}}')
    selected = commit.manifest_row()
    blob = tmp_path / record.blob_key
    if damage == "missing":
        blob.unlink()
    elif damage == "gzip":
        blob.write_bytes(b"broken")
    elif damage == "hash":
        blob.write_bytes(gzip.compress(b"{}"))
    elif damage == "json":
        record = store.store(target, b"invalid json", fetched_at=OLD.isoformat())
        selected.update(content_hash=record.content_hash, raw_uri=record.raw_uri)
    else:
        selected["raw_uri"] += ".wrong"
    transport, session = _transport([], raw_store=store)
    assert not transport.replay_target_key(target.target_key, observation=selected).ok
    assert not session.calls


@pytest.mark.parametrize("mode", [RunMode.BACKFILL, RunMode.REPLAY])
def test_selected_season_replay_ignores_unaccepted_pointer(tmp_path, mode):
    store = FotMobRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target("leagues", {"id": 47, "season": "2025/2026"})
    record, commit = _selected(store, target, _league_payload())
    repository = MemoryFotMobRepository()
    repository.record(replace(commit, target_type="league_season", entity_id=None))
    store.store(target, json.dumps(_league_payload("2024/2025")).encode())
    transport, session = _transport([], raw_store=store)
    service = FotMobIngestService(transport=transport, repository=repository, mode=mode)
    result, bundle = service.sync_season(47, "2025/2026")
    assert result.ok and bundle is not None, result.errors
    assert repository.commits[-1].content_hash == record.content_hash
    assert not session.calls


@pytest.mark.parametrize("mode", [RunMode.BACKFILL, RunMode.REPLAY])
def test_wrong_cached_season_gets_one_live_retry_only_in_backfill(tmp_path, mode):
    store = FotMobRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target("leagues", {"id": 47, "season": "2025/2026"})
    _, commit = _selected(store, target, _league_payload("2024/2025"))
    repository = MemoryFotMobRepository()
    repository.record(replace(commit, target_type="league_season", entity_id=None))
    transport, session = _transport([FakeResponse(200, json.dumps(_league_payload()).encode())], raw_store=store)
    service = FotMobIngestService(transport=transport, repository=repository, mode=mode)
    result, bundle = service.sync_season(47, "2025/2026")
    assert result.ok == (mode == RunMode.BACKFILL)
    assert (bundle is not None) == (mode == RunMode.BACKFILL)
    assert len(session.calls) == (1 if mode == RunMode.BACKFILL else 0)


@pytest.mark.parametrize("metadata", [{}, {"source_observed_at": OLD.isoformat()},
                                      {"validated_at": OLD.isoformat()}])
def test_legacy_freshness_never_uses_replay_completion(metadata):
    row = {"fetched_at": OLD, "completed_at": datetime.now(timezone.utc),
           "capabilities_json": json.dumps(metadata)}
    assert _validated_at(row) == OLD.replace(tzinfo=None)
    assert _validated_at({**row, "stale": True}) is None


def test_offline_replay_preserves_validation_and_expired_team_ttl(tmp_path):
    store = FotMobRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target("teams", {"id": 1})
    payload = {"details": {"id": 1, "name": "A"}, "overview": {}, "squad": {"squad": []}}
    _, commit = _selected(store, target, payload)
    transport, session = _transport([FakeResponse(200, json.dumps(payload).encode())], raw_store=store)
    repository = MemoryFotMobRepository()
    service = FotMobIngestService(transport=transport, repository=repository, mode=RunMode.REPLAY)
    replay = transport.replay_target_key(target.target_key, observation=commit.manifest_row())
    service._commit_for_fetch(replay, target_type="team", entity_id=1)
    row = repository.latest_entity_success("team", 1)
    assert datetime.fromisoformat(json.loads(row["capabilities_json"])["validated_at"]).replace(tzinfo=None) == OLD.replace(tzinfo=None)
    assert _validated_at(row) == OLD.replace(tzinfo=None)
    from scrapers.fotmob.domain import ScopeRef
    from scrapers.fotmob.parsers import parse_season_bundle
    bundle = parse_season_bundle(_league_payload(), ScopeRef(47, "2025/2026"))
    # Isolate the accepted team from other calendar participants.
    bundle = replace(bundle, teams=tuple(row for row in bundle.teams if str(row["team_id"]) == "1"))
    service.mode = RunMode.DAILY
    result, _ = service.sync_team_snapshots(bundle)
    assert result.ok and result.succeeded == 1, result.errors
    assert len(session.calls) == 1


def test_304_manifest_keeps_confirmation_and_stale_does_not_refresh_it(tmp_path):
    store = FotMobRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target("teams", {"id": 1})
    store.store(target, b'{"details":{"id":1}}', fetched_at=OLD.isoformat())
    transport, _ = _transport([FakeResponse(304)], raw_store=store)
    fetch = transport.fetch_json(target.canonical_url)
    repository = MemoryFotMobRepository()
    service = FotMobIngestService(transport=transport, repository=repository)
    service._commit_for_fetch(fetch, target_type="team", entity_id=1)
    row = repository.latest_entity_success("team", 1)
    assert _validated_at(row) > OLD.replace(tzinfo=None)
    assert _validated_at(row) == datetime.fromisoformat(fetch.validated_at).replace(tzinfo=None)
    service._commit_for_fetch(replace(fetch, stale=True), target_type="team", entity_id=1)
    assert _validated_at(repository.commits[-1].manifest_row()) is None


@pytest.mark.parametrize("table", sorted(REPLACE_TARGET_CURRENT_TABLES))
@pytest.mark.parametrize("metadata", [False, True])
@pytest.mark.parametrize("newer_status", ["success", "not_available"])
def test_current_orders_every_replacement_by_source_time(table, metadata, newer_status):
    _assert_current_source_order(table, metadata, newer_status)


@pytest.mark.parametrize("table", ["fotmob_leaderboards", "fotmob_match_payloads"])
def test_legacy_rawless_tombstone_hides_previous_rows_and_later_success_returns(table):
    _assert_current_source_order(table, True, "legacy_not_available")


def _assert_current_source_order(table, metadata, newer_status):
    target_type, keys = CURRENT_VIEW_SPECS[table]
    with duckdb.connect() as db:
        db.execute("CREATE SCHEMA bronze")
        db.execute("CREATE MACRO from_iso8601_timestamp(value) AS CAST(value AS TIMESTAMPTZ)")
        db.execute("CREATE MACRO json_extract_scalar(value, path) AS json_extract_string(value, path)")
        db.execute("""CREATE TABLE bronze.fotmob_ingest_manifest (
            batch_id VARCHAR, target_key VARCHAR, parser_version VARCHAR, status VARCHAR,
            target_type VARCHAR, entity_id VARCHAR, competition_id VARCHAR,
            source_season_key VARCHAR, capabilities_json VARCHAR,
            fetched_at TIMESTAMP, completed_at TIMESTAMP)""")
        definitions = [f'"{key}" VARCHAR' for key in keys]
        definitions += ["_target_batch_id VARCHAR", "_observed_at TIMESTAMP", "_ingested_at TIMESTAMP"]
        db.execute(f"CREATE TABLE bronze.{table} ({', '.join(definitions)})")
        for batch, observed, completed in (
            ("newer", OLD + timedelta(hours=2), OLD + timedelta(hours=3)),
            ("older", OLD + timedelta(hours=1), OLD + timedelta(hours=4)),
        ):
            capabilities = json.dumps({"source_observed_at": observed.isoformat()}) if metadata else "{}"
            db.execute("INSERT INTO bronze.fotmob_ingest_manifest VALUES (?, 'target', ?, 'success', ?, '1', '1', '1', ?, ?, ?)",
                       [batch, PARSER_VERSION, target_type.split(",")[0], capabilities,
                        OLD if metadata else observed, completed])
            values = ["1"] * len(keys) + [batch, observed, completed]
            db.execute(f"INSERT INTO bronze.{table} VALUES ({', '.join('?' for _ in values)})", values)

        if table == "fotmob_match_payloads":
            db.execute("CREATE TABLE bronze.fotmob_matches (competition_id VARCHAR, source_season_key VARCHAR, match_id VARCHAR, _target_batch_id VARCHAR, _observed_at TIMESTAMP, _ingested_at TIMESTAMP)")
            db.execute("INSERT INTO bronze.fotmob_matches VALUES ('1', '1', '1', 'calendar', ?, ?)", [OLD, OLD])
            db.execute("INSERT INTO bronze.fotmob_ingest_manifest VALUES ('calendar', 'calendar', ?, 'success', 'league_season', NULL, '1', '1', '{}', ?, ?)", [PARSER_VERSION, OLD, OLD])

        if newer_status != "success":
            db.execute("UPDATE bronze.fotmob_ingest_manifest SET status = 'not_available' WHERE batch_id = 'newer'")
        if newer_status == "legacy_not_available":
            # sync_leaderboards writes missing_fetch_all_url without source time.
            db.execute("UPDATE bronze.fotmob_ingest_manifest SET fetched_at = NULL, capabilities_json = '{}' WHERE batch_id = 'newer'")
            db.execute("UPDATE bronze.fotmob_ingest_manifest SET completed_at = ? WHERE batch_id = 'older'", [OLD + timedelta(hours=2)])
            db.execute(f"DELETE FROM bronze.{table} WHERE _target_batch_id = 'newer'")

        class Trino:
            def table_exists(self, schema, name):
                return name == table or (table == "fotmob_match_payloads" and name == "fotmob_matches")

            def get_table_columns(self, schema, name):
                return [row[0] for row in db.execute(f"DESCRIBE bronze.{name}").fetchall()]

            def _execute(self, sql):
                db.execute(sql.replace("iceberg.bronze.", "bronze."))

        class Writer:
            def _get_trino_manager(self):
                return Trino()

        FotMobRepository(writer=Writer()).ensure_current_views()
        expected = [("newer",)] if newer_status == "success" else []
        assert db.execute(f"SELECT _target_batch_id FROM bronze.{table}_current").fetchall() == expected
        if newer_status == "legacy_not_available":
            # A source observation after the absence decision restores current.
            later = OLD + timedelta(hours=5)
            db.execute("UPDATE bronze.fotmob_ingest_manifest SET fetched_at = ?, capabilities_json = '{}' WHERE batch_id = 'older'", [later])
            assert db.execute(f"SELECT _target_batch_id FROM bronze.{table}_current").fetchall() == [("older",)]


def test_prefetched_season_alias_uses_verified_observation(tmp_path):
    store = FotMobRawStore.from_uri(tmp_path.as_uri())
    source = canonicalize_target("leagues", {"id": 47})
    record, commit = _selected(store, source, _league_payload())
    transport, session = _transport([], raw_store=store)
    prefetched = transport.replay_target_key(source.target_key, observation=commit.manifest_row())
    store.store(source, json.dumps(_league_payload("2024/2025")).encode())
    repository = MemoryFotMobRepository()
    service = FotMobIngestService(transport=transport, repository=repository)
    result, bundle = service.sync_season(47, "2025/2026", prefetched=prefetched)
    assert result.ok and bundle is not None, result.errors
    exact = canonicalize_target("leagues", {"id": 47, "season": "2025/2026"})
    _, aliased = store.load(exact)
    assert aliased.content_hash == record.content_hash
    assert aliased.validated_at == OLD.isoformat()
    assert not session.calls


def test_repository_indexes_keep_newer_confirmation_and_current_run_overlay():
    repository = FotMobRepository(writer=object())
    repository._preloaded = True
    repository._preloaded_run_id = "delayed"
    target = canonicalize_target("teams", {"id": 1})
    newer = TargetCommit(
        run_id="accepted", target_type="team", entity_id="1",
        target_key=target.target_key, status=ManifestStatus.NOT_MODIFIED,
        fetched_at=OLD, completed_at=OLD + timedelta(hours=3),
        capabilities={"source_observed_at": (OLD + timedelta(hours=2)).isoformat()},
    )
    older = replace(newer, run_id="delayed", status=ManifestStatus.SUCCESS,
                    completed_at=OLD + timedelta(hours=4),
                    capabilities={"source_observed_at": (OLD + timedelta(hours=1)).isoformat()})
    repository._index_committed(newer.manifest_row())
    repository._index_pending(older.manifest_row())
    for reader, args in ((repository.latest_success, (target.target_key,)),
                         (repository.latest_entity_success, ("team", 1))):
        assert reader(*args)["run_id"] == "accepted"
        assert reader(*args, run_id="delayed")["run_id"] == "delayed"
    repository._index_committed(older.manifest_row())
    repository._pending_targets.clear()
    repository._pending_entities.clear()
    assert repository.latest_entity_success("team", 1)["run_id"] == "accepted"
    assert repository.latest_entity_success("team", 1, run_id="delayed")["run_id"] == "delayed"

    memory = MemoryFotMobRepository()
    memory.record(newer)
    memory.record(older)
    assert memory.latest_entity_success("team", 1)["run_id"] == "accepted"
    assert memory.latest_success(target.target_key)["run_id"] == "accepted"


@pytest.mark.parametrize("status", [204, 404])
def test_live_absence_supersedes_accepted_success(tmp_path, status):
    store = FotMobRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target("teams", {"id": 1})
    _, accepted = _selected(store, target, {"details": {"id": 1}})
    repository = MemoryFotMobRepository()
    repository.record(replace(accepted, target_type="team", entity_id="1"))
    transport, _ = _transport([FakeResponse(status)], raw_store=store)
    service = FotMobIngestService(transport=transport, repository=repository)
    fetch = transport.fetch_json(target.canonical_url)
    # Entity-scoped proof opts into absence; a bare HTTP failure does not.
    service._commit_for_fetch(fetch, target_type="team", entity_id=1,
                              status=ManifestStatus.NOT_AVAILABLE)
    assert repository.latest_success(target.target_key) is None
    assert repository.latest_entity_success("team", 1) is None
    assert repository.latest_entity_raw_target("team", 1) is None
    row = repository.commits[-1].manifest_row()
    assert datetime.fromisoformat(json.loads(row["capabilities_json"])["source_observed_at"]) > OLD


def test_selected_stale_replay_keeps_unvalidated_provenance(tmp_path):
    store = FotMobRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target("teams", {"id": 1})
    _, commit = _selected(store, target, {"details": {"id": 1}})
    transport, session = _transport([], raw_store=store)
    fetch = transport.replay_target_key(target.target_key, observation=replace(commit, stale=True).manifest_row())
    assert fetch.ok and fetch.stale
    assert fetch.source_observed_at is None
    repository = MemoryFotMobRepository()
    service = FotMobIngestService(transport=transport, repository=repository)
    service._commit_for_fetch(fetch, target_type="team", entity_id=1)
    assert _validated_at(repository.commits[-1].manifest_row()) is None
    assert not session.calls


@pytest.mark.parametrize("target_type", ["leaderboard", "match"])
def test_rawless_tombstone_source_rank_matches_memory_and_repository(target_type):
    target = canonicalize_target("teams", {"id": 1})
    success = TargetCommit(
        run_id="first", target_type=target_type, entity_id="1",
        target_key=target.target_key, status=ManifestStatus.SUCCESS,
        fetched_at=OLD, completed_at=OLD + timedelta(hours=4),
    )
    tombstone = replace(success, run_id="absent", status=ManifestStatus.NOT_AVAILABLE,
                        fetched_at=None, completed_at=OLD + timedelta(hours=2))
    later = replace(success, run_id="returns", fetched_at=OLD + timedelta(hours=3))
    repository = FotMobRepository(writer=object())
    repository._preloaded = True
    memory = MemoryFotMobRepository()
    for commit, expected_run in ((success, "first"), (tombstone, None), (later, "returns")):
        repository._index_committed(commit.manifest_row())
        memory.record(commit)
        for repo in (repository, memory):
            for row in (repo.latest_success(target.target_key),
                        repo.latest_entity_success(target_type, 1)):
                assert (row["run_id"] if row else None) == expected_run


def test_validation_without_source_observed_at_orders_304_after_old_fetch(tmp_path):
    store = FotMobRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target("teams", {"id": 1})
    store.store(target, b'{"details":{"id":1}}', fetched_at=OLD.isoformat())
    transport, _ = _transport([FakeResponse(304)], raw_store=store)
    repository = MemoryFotMobRepository()
    service = FotMobIngestService(transport=transport, repository=repository)
    target = canonicalize_target("teams", {"id": 1})
    commit = TargetCommit(
        run_id="old", target_type="team", entity_id="1",
        target_key=target.target_key, status=ManifestStatus.SUCCESS,
        fetched_at=OLD, completed_at=OLD + timedelta(hours=4),
    )
    repository.record(commit)
    fetch = replace(
        transport.fetch_json(target.canonical_url), source_observed_at=None,
        validated_at=(OLD + timedelta(hours=2)).isoformat(),
    )
    service._commit_for_fetch(fetch, target_type="team", entity_id=1)
    row = repository.latest_success(target.target_key)
    assert row["run_id"] == service.run_id
    assert json.loads(row["capabilities_json"])["source_observed_at"] == fetch.validated_at


def test_memory_latest_readers_use_last_record_when_manifest_ranks_tie():
    repository = MemoryFotMobRepository()
    target = canonicalize_target("teams", {"id": 1})
    commit = TargetCommit(
        run_id="same-run", target_type="team", entity_id="1",
        target_key=target.target_key, status=ManifestStatus.SUCCESS,
        fetched_at=OLD, completed_at=OLD, stale=True,
    )
    repository.record(commit)
    repository.record(replace(commit, stale=False))
    assert repository.latest_success(target.target_key)["stale"] is False
    assert repository.latest_entity_success("team", 1)["stale"] is False
