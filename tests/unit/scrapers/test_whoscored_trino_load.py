"""Offline regressions for WhoScored's Trino identity and shared history reads."""

import multiprocessing
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from scrapers.whoscored import history
from scrapers.whoscored.catalog import (
    CatalogCompetition,
    CatalogSeason,
    WhoScoredCatalog,
)
from scrapers.whoscored.domain import WhoScoredScope
from scrapers.whoscored.repository import WhoScoredRepository


def test_repository_reads_and_writes_reconnect_with_source_identity(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("TRINO_USER", "src_whoscored")
    monkeypatch.setenv("TRINO_PASSWORD", "shared-fixture")
    monkeypatch.setenv("TRINO_SRC_WHOSCORED_PASSWORD", "source-fixture")
    monkeypatch.setenv("WHOSCORED_LOCK_DIR", str(tmp_path))
    with (
        patch("trino.dbapi.connect") as connect,
        patch("trino.auth.BasicAuthentication") as auth,
    ):
        repository = WhoScoredRepository()
        repository.trino.execute_query("SELECT 1")
        writer_manager = repository.writer._get_trino_manager()
        writer_manager._reset_connection()
        writer_manager._execute(
            "INSERT INTO iceberg.bronze.whoscored_matches VALUES (1)"
        )
    assert [item.kwargs["user"] for item in connect.call_args_list] == [
        "src_whoscored"
    ] * 2
    assert [item.args for item in auth.call_args_list] == [
        ("src_whoscored", "source-fixture")
    ] * 2


def _history_executor(directory, has_work):
    league = "fixture-league"
    seasons = [
        CatalogSeason(
            WhoScoredScope(league, slug, "split_year"),
            source_season_id=i,
            stage_ids=(1,),
            is_active=True,
        )
        for i, slug in enumerate(("2425", "2526", "2627"), 1)
    ]
    catalog = WhoScoredCatalog(
        (CatalogCompetition(league, tuple(seasons), True, tournament_id=2),)
    )
    connection, cursor = MagicMock(), MagicMock()
    connection.cursor.return_value = cursor

    def execute(sql, *args):
        if "whoscored_scope_ingest_manifest" not in sql.lower():
            cursor.fetchall.return_value = [(1,)]
            return
        coverage = "closeable" in sql.lower()
        with (directory / "polls").open("a") as events:
            print("coverage" if coverage else "season", file=events)
        cursor.fetchall.return_value = (
            [(league, s.scope.season_id, 1, 0, 0, 0, [1], 0, None) for s in seasons]
            if coverage
            else [('{"__checked_stage_ids__":[1]}',)]
        )

    cursor.execute.side_effect = execute
    with patch("trino.dbapi.connect", return_value=connection):
        repository = WhoScoredRepository()
        repository.list_match_candidates = MagicMock(return_value=[1])
        repository.list_profile_candidates = MagicMock(return_value=[])
        repository.list_preview_candidates = MagicMock(return_value=[])
        checkpoint = MagicMock()
        checkpoint.reserve.side_effect = lambda items, now: (items[0], "fixture")
        checkpoint.parked.return_value = 0
        service = MagicMock()
        service.return_value.__enter__.return_value.sync_matches.return_value.attempted = 1
        report = history.run_chunk(
            catalog=catalog,
            repository=repository,
            checkpoint=checkpoint,
            service_cls=service,
            enabled=lambda: True,
            current_running=lambda: False,
            consume=lambda result: None,
            owns=lambda key: has_work,
            now=datetime(2026, 11, 19, tzinfo=timezone.utc),
        )
        assert bool(report.get("did_work")) == has_work


def test_three_history_executors_share_one_manifest_poll_including_pace(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("TRINO_USER", "src_whoscored")
    monkeypatch.setenv("TRINO_SRC_WHOSCORED_PASSWORD", "source-fixture")
    monkeypatch.setenv("WHOSCORED_LOCK_DIR", str(tmp_path))
    context = multiprocessing.get_context("spawn")
    for has_work in (True, False, False):
        process = context.Process(target=_history_executor, args=(tmp_path, has_work))
        process.start()
        try:
            process.join(20)
            assert process.exitcode == 0
        finally:
            if process.is_alive():
                process.terminate()
                process.join(20)
    polls = (tmp_path / "polls").read_text().splitlines()
    assert (polls.count("coverage"), polls.count("season")) == (1, 0)
