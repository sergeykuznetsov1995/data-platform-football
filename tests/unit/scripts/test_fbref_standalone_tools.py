from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from scrapers.fbref.fetcher import FETCHER_VERSION, FetchResponse
from scrapers.fbref.raw_store import RawPageStore


ROOT = Path(__file__).resolve().parents[3]


def _load_tool(name: str, relative: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


test_match_parser = _load_tool(
    "standalone_test_match_parser", "scripts/test_match_parser.py"
)
bench_fbref_fetch = _load_tool(
    "standalone_bench_fbref_fetch",
    "scripts/research/bench_fbref_fetch.py",
)


@pytest.mark.unit
def test_benchmark_keeps_legacy_keys_with_fetch_response_metrics(
    monkeypatch, tmp_path
) -> None:
    import scrapers.fbref.fetcher as fetcher_module

    body = b"<html>" + (b"x" * 50_001) + b"</html>"

    class FakeFetcher:
        def __init__(self, **kwargs) -> None:
            assert kwargs == {"proxy_file": None}

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> bool:
            return False

        def fetch(self, url: str, *, page_kind: str) -> FetchResponse:
            assert url.startswith("https://fbref.com/en/matches/")
            assert page_kind == "match"
            return FetchResponse(
                url=url,
                status_code=200,
                body=body,
                headers={"content-type": "text/html"},
                latency_ms=25,
                http_wire_bytes=42,
                decoded_html_bytes=len(body),
                browser_document_bytes=100,
                browser_asset_bytes=50,
                browser_requests=3,
                provider_billed_bytes=200,
            )

    report_path = tmp_path / "bench.json"
    monkeypatch.setattr(fetcher_module, "FBrefFetcher", FakeFetcher)
    monkeypatch.setattr(
        bench_fbref_fetch,
        "MATCH_PATHS",
        ["/en/matches/a071faa8/Fixture"],
    )
    monkeypatch.setattr(bench_fbref_fetch, "REPORT_PATH", str(report_path))
    monkeypatch.setattr(bench_fbref_fetch, "PROXY_FILE", "")
    monkeypatch.setattr(bench_fbref_fetch, "HTML_DIR", None)
    monkeypatch.setenv("FBREF_BENCH_MATCH_LIMIT", "1")

    bench_fbref_fetch.main()

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["transport"] == FETCHER_VERSION
    assert report["matches_succeeded"] == 1
    assert report["real_requests"] == 3
    assert report["http_requests"] == 1
    assert report["browser_document_bytes"] == 100
    assert report["browser_asset_bytes"] == 50
    assert report["http_wire_bytes"] == 42
    assert report["decoded_html_bytes"] == len(body)
    assert report["provider_billed_bytes"] == 200
    assert report["total_proxy_bytes"] == 192
    assert report["http_fetch_ok_total"] == 1
    assert report["http_fetch_fallback_total"] == 0
    assert report["per_match"][0]["http_latency_ms"] == 25


@pytest.mark.unit
def test_match_tool_commits_exact_bytes_before_returning(tmp_path) -> None:
    body = b"<html><body>committed</body></html>"
    response = FetchResponse(
        url="https://fbref.com/en/matches/a071faa8",
        status_code=200,
        body=body,
        headers={"etag": '"v1"'},
        latency_ms=12,
        http_wire_bytes=44,
        decoded_html_bytes=len(body),
    )
    fetcher = MagicMock()
    fetcher.fetch.return_value = response
    store = RawPageStore.from_uri(tmp_path.as_uri())

    returned = test_match_parser.fetch_and_commit_match(
        fetcher,
        store,
        "a071faa8",
        run_id="manual-test",
    )

    assert returned == body
    assert test_match_parser.load_committed_match(store, "a071faa8") == body
    fetcher.fetch.assert_called_once_with(
        "https://fbref.com/en/matches/a071faa8",
        page_kind="match",
    )
    _, record = store.load_fetch("manual-test:match:a071faa8")
    assert record.wire_bytes == 44
    assert record.latency_ms == 12
    assert record.etag == '"v1"'


@pytest.mark.unit
def test_match_tool_persists_committed_bytes_through_typed_adapter() -> None:
    body = b"<html><body>raw</body></html>"
    adapter = MagicMock()
    adapter.ingest_match_html.return_value = (
        object(),
        {"match_events": 4, "match_player_stats": 22},
    )

    result = test_match_parser.save_to_iceberg(
        ["a071faa8"],
        {"a071faa8": body},
        source_competition_id="9",
        source_season_id="2025-2026",
        competition_name="Premier League",
        compatibility_season=2025,
        run_id="manual-test",
        adapter=adapter,
    )

    assert result == {
        "a071faa8": {"match_events": 4, "match_player_stats": 22}
    }
    call = adapter.ingest_match_html.call_args
    assert call.args == (body,)
    assert call.kwargs["match_id"] == "a071faa8"
    assert call.kwargs["run_id"] == "manual-test"
    assert call.kwargs["target_identity"] == "manual-match:a071faa8"
    assert call.kwargs["context"].source_competition_id == "9"
    assert call.kwargs["context"].source_season_id == "2025-2026"


@pytest.mark.unit
@pytest.mark.parametrize(
    "relative",
    [
        "scripts/research/bench_fbref_fetch.py",
        "scripts/test_match_parser.py",
    ],
)
def test_tools_have_no_legacy_scraper_import(relative: str) -> None:
    source = (ROOT / relative).read_text(encoding="utf-8")
    assert "FBrefScraper" not in source
    assert "scrapers.nodriver_fbref" not in source
    assert "._fetch_page(" not in source
