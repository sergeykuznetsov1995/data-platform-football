"""Offline-only contracts for the two FBref migration tools."""

from __future__ import annotations

import ast
from contextlib import contextmanager
import importlib.util
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

SOURCE_RUN_ID = "12345678-1234-5678-1234-567812345678"
ROOT = Path(__file__).resolve().parents[3]


def _load_script(module_name: str, filename: str):
    spec = importlib.util.spec_from_file_location(
        module_name, ROOT / "scripts" / filename
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


player_stats = _load_script(
    "_test_rescrape_match_player_stats", "rescrape_match_player_stats.py"
)
managers = _load_script(
    "_test_backfill_fbref_match_managers", "backfill_fbref_match_managers.py"
)


def _candidate(index: int) -> dict[str, object]:
    return {
        "target_id": f"fbref:match:0000000{index}",
        "logical_refresh_id": str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"refresh:{index}")
        ),
        "content_hash": f"hash-{index}",
        "page_kind": "match",
    }


def _record(item: dict[str, object]) -> SimpleNamespace:
    match_id = str(item["target_id"]).rsplit(":", 1)[-1]
    return SimpleNamespace(
        logical_refresh_id=item["logical_refresh_id"],
        target_id=item["target_id"],
        content_hash=item["content_hash"],
        source="fbref",
        page_kind="match",
        source_ids={
            "match_id": match_id,
            "competition_id": "9",
            "season_id": "2025-2026",
            "competition_name": "Premier League",
        },
    )


class FakeControl:
    def __init__(
        self,
        candidates,
        *,
        known=True,
        run_type="current",
        status="succeeded",
        latest=True,
    ):
        self.candidates = list(candidates)
        self.known = known
        self.run_type = run_type
        self.status = status
        self.latest = latest
        self.manifests: list[dict[str, object]] = []
        self.observations: dict[str, str] = {}

    def get_run(self, run_id):
        if not self.known:
            return None
        return {
            "run_id": run_id,
            "run_type": self.run_type,
            "status": self.status,
        }

    @contextmanager
    def guard_latest_content(
        self, target_id, content_hash, logical_refresh_id
    ):
        yield self.latest

    def list_replay_fetches(
        self,
        run_id,
        *,
        parser_version,
        typed_parser_version,
        stateful_parser_version,
        page_kinds,
        limit,
    ):
        assert run_id == SOURCE_RUN_ID
        assert page_kinds == ["match"]
        assert stateful_parser_version == (
            player_stats.REMEDIATION_STATEFUL_VERSION
        )
        return [
            item
            for item in self.candidates
            if self.observations.get(str(item["logical_refresh_id"]))
            != "succeeded"
        ][:limit]

    def claim_observation_processing(self, **kwargs):
        refresh = str(kwargs["logical_refresh_id"])
        if self.observations.get(refresh) in {"processing", "succeeded"}:
            return None
        self.observations[refresh] = "processing"
        return SimpleNamespace(logical_refresh_id=refresh)

    def complete_observation_processing(self, lease, **kwargs):
        self.observations[lease.logical_refresh_id] = "succeeded"

    def fail_observation_processing(self, lease, **kwargs):
        self.observations[lease.logical_refresh_id] = "failed"

    def list_run_fetches(self, run_id, **kwargs):
        assert run_id == SOURCE_RUN_ID
        assert kwargs["page_kinds"] == ["match"]
        assert kwargs["only_unparsed"] is False
        return self.candidates[: kwargs["limit"]]

    def record_dataset_manifest(self, **kwargs):
        self.manifests.append(kwargs)


class FakeRawStore:
    def __init__(self, records):
        self.records = dict(records)
        self.loads: list[str] = []

    def load_fetch_html(self, logical_refresh_id):
        self.loads.append(str(logical_refresh_id))
        return "<html><body>stored</body></html>", self.records[logical_refresh_id]


class FakeAdapter:
    def __init__(self):
        self.calls: list[dict[str, object]] = []

    def ingest_match_html(self, html, **kwargs):
        self.calls.append({"html": html, **kwargs})
        enabled = set(kwargs["enabled_datasets"])
        datasets = {
            name: SimpleNamespace(status=SimpleNamespace(value="available"))
            for name in enabled
        }
        counts = {
            name: (2 if name == managers.MANAGERS_DATASET else 22)
            for name in enabled
        }
        return SimpleNamespace(datasets=datasets), counts


def _dependencies(count=2):
    candidates = [_candidate(index) for index in range(1, count + 1)]
    return (
        FakeControl(candidates),
        FakeRawStore(
            {item["logical_refresh_id"]: _record(item) for item in candidates}
        ),
        FakeAdapter(),
    )


@pytest.mark.unit
def test_player_stats_replay_is_bounded_incremental_and_network_zero():
    control, raw_store, adapter = _dependencies()

    result = player_stats.run_offline_match_replay(
        source_control_run_id=SOURCE_RUN_ID,
        target_dataset=player_stats.PLAYER_STATS_DATASET,
        parser_version=player_stats.PLAYER_STATS_PARSER_VERSION,
        max_pages=1,
        control=control,
        raw_store=raw_store,
        adapter=adapter,
    )

    assert result.selected == result.processed == 1
    assert result.rows_written == 22
    assert result.remaining is True
    assert result.network_requests == 0
    assert len(raw_store.loads) == 1
    assert adapter.calls[0]["enabled_datasets"] == {"match_player_stats"}
    assert adapter.calls[0]["require_player_contract"] is True
    assert control.manifests[-1]["dataset"] == "__page__"
    assert control.manifests[-1]["validation_status"] == "succeeded"


@pytest.mark.unit
def test_manager_wrapper_uses_same_raw_completion_contract():
    control, raw_store, adapter = _dependencies(count=1)

    exit_code = managers.main(
        ["--source-control-run-id", SOURCE_RUN_ID, "--max-pages", "1"],
        control=control,
        raw_store=raw_store,
        adapter=adapter,
    )

    assert exit_code == 0
    assert adapter.calls[0]["enabled_datasets"] == {
        "match_managers",
        "match_player_stats",
    }
    assert control.manifests[-2]["dataset"] == "remediation:match_managers"
    assert control.manifests[-1]["dataset"] == "__page__"


@pytest.mark.unit
def test_dry_run_does_not_open_raw_store_or_typed_writer():
    control, _, _ = _dependencies(count=1)

    result = player_stats.run_offline_match_replay(
        source_control_run_id=SOURCE_RUN_ID,
        target_dataset=player_stats.PLAYER_STATS_DATASET,
        parser_version=player_stats.PLAYER_STATS_PARSER_VERSION,
        max_pages=1,
        dry_run=True,
        control=control,
        raw_store=SimpleNamespace(load_fetch_html=lambda _value: pytest.fail()),
        adapter=SimpleNamespace(ingest_match_html=lambda *_a, **_kw: pytest.fail()),
    )

    assert result.dry_run is True
    assert result.processed == 0
    assert result.network_requests == 0
    assert control.manifests == []


@pytest.mark.unit
def test_cli_rejects_missing_uuid_and_more_than_25_pages():
    with pytest.raises(SystemExit) as missing:
        player_stats.main([])
    assert missing.value.code == 2

    with pytest.raises(SystemExit) as unbounded:
        managers.main(
            [
                "--source-control-run-id",
                SOURCE_RUN_ID,
                "--max-pages",
                "26",
            ]
        )
    assert unbounded.value.code == 2


@pytest.mark.unit
def test_unknown_source_run_fails_before_raw_or_writer_access():
    exit_code = player_stats.main(
        ["--source-control-run-id", SOURCE_RUN_ID],
        control=FakeControl([], known=False),
        raw_store=SimpleNamespace(load_fetch_html=lambda _value: pytest.fail()),
        adapter=SimpleNamespace(ingest_match_html=lambda *_a, **_kw: pytest.fail()),
    )
    assert exit_code == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    ("run_type", "status", "message"),
    [
        ("replay", "succeeded", "not replayable"),
        ("current", "pending", "not terminal"),
        ("backfill", "running", "not terminal"),
    ],
)
def test_active_or_replay_source_fails_before_raw_or_writer_access(
    run_type, status, message
):
    control = FakeControl([], run_type=run_type, status=status)
    with pytest.raises(player_stats.OfflineRemediationError, match=message):
        player_stats.run_offline_match_replay(
            source_control_run_id=SOURCE_RUN_ID,
            target_dataset=player_stats.PLAYER_STATS_DATASET,
            parser_version=player_stats.PLAYER_STATS_PARSER_VERSION,
            control=control,
            raw_store=SimpleNamespace(
                load_fetch_html=lambda _value: pytest.fail()
            ),
            adapter=SimpleNamespace(
                ingest_match_html=lambda *_a, **_kw: pytest.fail()
            ),
        )


@pytest.mark.unit
def test_stale_raw_is_completed_without_typed_replacement():
    candidates = [_candidate(1)]
    control = FakeControl(candidates, latest=False)
    raw_store = FakeRawStore(
        {candidates[0]["logical_refresh_id"]: _record(candidates[0])}
    )
    adapter = FakeAdapter()

    result = player_stats.run_offline_match_replay(
        source_control_run_id=SOURCE_RUN_ID,
        target_dataset=player_stats.PLAYER_STATS_DATASET,
        parser_version=player_stats.PLAYER_STATS_PARSER_VERSION,
        control=control,
        raw_store=raw_store,
        adapter=adapter,
    )

    assert result.processed == 1
    assert result.rows_written == 0
    assert adapter.calls == []
    assert control.manifests == []
    refresh = str(candidates[0]["logical_refresh_id"])
    assert control.observations[refresh] == "succeeded"


@pytest.mark.unit
def test_raw_control_mismatch_records_failure_and_never_completes_page():
    control, raw_store, adapter = _dependencies(count=1)
    item = control.candidates[0]
    raw_store.records[item["logical_refresh_id"]].target_id = "wrong-target"

    with pytest.raises(player_stats.OfflineRemediationError, match="mismatch"):
        player_stats.run_offline_match_replay(
            source_control_run_id=SOURCE_RUN_ID,
            target_dataset=player_stats.PLAYER_STATS_DATASET,
            parser_version=player_stats.PLAYER_STATS_PARSER_VERSION,
            max_pages=1,
            control=control,
            raw_store=raw_store,
            adapter=adapter,
        )

    assert adapter.calls == []
    assert control.manifests[-1]["dataset"] == "__page__"
    assert control.manifests[-1]["validation_status"] == "failed"
    assert not any(
        item["dataset"] == "__page__"
        and item["validation_status"] == "succeeded"
        for item in control.manifests
    )


@pytest.mark.unit
def test_tools_have_no_scraper_fetcher_browser_or_parallel_crawl_imports():
    forbidden_modules = {
        "concurrent.futures",
        "selenium",
        "nodriver",
        "scrapers.fbref.fetcher",
        "scrapers.fbref.scraper",
    }
    forbidden_calls = {"fetch", "fetch_wave", "_fetch_page"}

    for relative in (
        "scripts/backfill_fbref_match_managers.py",
        "scripts/rescrape_match_player_stats.py",
    ):
        tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
        imported = set()
        calls = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add(node.module or "")
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    calls.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    calls.add(node.func.attr)
        assert not (imported & forbidden_modules), relative
        assert not (calls & forbidden_calls), relative
