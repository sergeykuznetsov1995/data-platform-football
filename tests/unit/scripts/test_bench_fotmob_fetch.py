from __future__ import annotations

import gzip
import importlib.util
import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import requests


REPO_ROOT = Path(__file__).resolve().parents[3]


def _load_script(name):
    path = REPO_ROOT / "scripts" / "research" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


bench = _load_script("bench_fotmob_fetch")


def _payload(*, competition_id=47, season="2025/2026"):
    return {
        "details": {
            "id": competition_id,
            "selectedSeason": season,
            "allAvailableSeasons": [season, "2024/2025"],
        },
        "fixtures": {
            "allMatches": [
                {"id": 10, "home": {"id": 1}, "away": {"id": 2}},
                {"id": 11, "home": {"id": 2}, "away": {"id": 1}},
            ]
        },
        "table": [
            {
                "data": {
                    "tables": [
                        {"name": "Group A", "table": {"all": [{"id": 1}, {"id": 2}]}},
                        {"name": "Best third", "table": {"all": [{"id": 2}]}},
                    ]
                }
            }
        ],
        "stats": {
            "players": [{"name": "goals"}, {"name": "assists"}],
            "teams": [{"name": "xg"}],
        },
        "playoff": {"matches": [{"id": 12}]},
    }


class FakeRaw:
    def __init__(self, body):
        self.body = body

    def read(self, decode_content=False):
        assert decode_content is False
        body, self.body = self.body, b""
        return body


class FakeResponse:
    def __init__(self, status, body, *, headers=None):
        self.status_code = status
        self.raw = FakeRaw(body)
        self.headers = headers or {}
        self.url = "https://www.fotmob.com/api/data/leagues?id=47&season=2025%2F2026"
        self.closed = False

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []
        self.closed = False

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def close(self):
        self.closed = True


def _json_response(payload, *, status=200, encoding="gzip", extra_headers=None):
    decoded = json.dumps(payload, separators=(",", ":")).encode()
    encoded = gzip.compress(decoded, mtime=0) if encoding == "gzip" else decoded
    headers = {
        "Content-Type": "application/json",
        "Content-Encoding": encoding if encoding != "identity" else "",
        "Content-Length": str(len(encoded)),
    }
    headers.update(extra_headers or {})
    return FakeResponse(status, encoded, headers=headers), encoded, decoded


def test_fixed_sentinel_matrix_has_exact_source_keys_and_qualifier():
    got = {
        (target.key, target.competition_id, target.source_season_key)
        for target in bench.SENTINELS
    }
    assert got == {
        ("epl", 47, "2025/2026"),
        ("ucl", 42, "2025/2026"),
        ("ucl_qualification", 10611, "2025/2026"),
        ("nations_a", 9806, "2024/2025"),
        ("rpl", 63, "2025/2026"),
        ("afcon", 289, "2025"),
    }


def test_source_summary_counts_groups_duplicates_categories_and_paths():
    payload = _payload()
    counts = bench.summarize_payload(payload)

    assert counts["matches_rows"] == 2
    assert counts["matches_unique"] == 2
    assert counts["standings_rows"] == 3
    assert counts["standings_unique_rows"] == 2
    assert counts["standings_tables"] == 2
    assert counts["playoff_match_rows"] == 1
    assert counts["player_categories"] == 2
    assert counts["team_categories"] == 1
    assert counts["available_seasons"] == 2
    assert "$/table/[]/data/tables/[]/table/all/[]/id" in bench.json_schema_paths(
        payload
    )


def test_fetch_counts_retry_encoded_and_decoded_direct_bytes():
    retry = FakeResponse(
        429,
        b"{}",
        headers={"Content-Encoding": "", "Retry-After": "0"},
    )
    success, encoded, decoded = _json_response(_payload())
    session = FakeSession(retry, success)
    slept = []

    capture = bench.fetch_scope(
        bench.SENTINELS[0],
        session=session,
        limiter=None,
        max_attempts=4,
        sleep=slept.append,
    )

    assert capture.record["ok"] is True
    assert capture.record["attempt_count"] == 2
    assert capture.record["retry_count"] == 1
    assert capture.record["status_counts"] == {"429": 1, "200": 1}
    assert capture.record["encoded_direct_bytes"] == 2 + len(encoded)
    assert capture.record["decoded_direct_bytes"] == 2 + len(decoded)
    assert capture.record["encoded_proxy_bytes"] == 0
    assert capture.record["selected_season_observed"] == "2025/2026"
    assert capture.record["competition_id_observed"] == 47
    assert slept == [0.0]
    assert all(response.closed for response in (retry, success))
    assert session.calls[0][1]["stream"] is True


def test_request_exception_is_retried_but_json_error_is_terminal():
    success, _, _ = _json_response(_payload())
    session = FakeSession(requests.ConnectionError("temporary"), success)

    capture = bench.fetch_scope(
        bench.SENTINELS[0],
        session=session,
        limiter=None,
        max_attempts=2,
        sleep=lambda _: None,
    )

    assert capture.record["ok"] is True
    assert capture.record["attempt_count"] == 2

    invalid = FakeResponse(200, b"not-json", headers={"Content-Encoding": ""})
    unused, _, _ = _json_response(_payload())
    capture = bench.fetch_scope(
        bench.SENTINELS[0],
        session=FakeSession(invalid, unused),
        limiter=None,
        max_attempts=4,
        sleep=lambda _: None,
    )
    assert capture.record["ok"] is False
    assert capture.record["attempt_count"] == 1
    assert "invalid_json" in capture.record["validation_errors"][0]


def test_exact_selected_season_mismatch_is_not_silent():
    response, _, _ = _json_response(_payload(season="2024/2025"))
    capture = bench.fetch_scope(
        bench.SENTINELS[0],
        session=FakeSession(response),
        limiter=None,
    )

    assert capture.payload is not None
    assert capture.record["ok"] is False
    assert capture.record["selected_season_observed"] == "2024/2025"
    assert capture.record["validation_errors"] == [
        "selected season mismatch: requested='2025/2026', observed='2024/2025'"
    ]


def test_missing_selected_season_can_only_be_downgraded_explicitly(tmp_path):
    payload = _payload()
    payload["details"].pop("selectedSeason")
    response, _, _ = _json_response(payload)

    report = bench.run_benchmark(
        scopes=[bench.SENTINELS[0]],
        artifact_dir=tmp_path,
        label="allow-missing",
        requests_per_minute=0,
        strict_selected_season=False,
        session=FakeSession(response),
    )

    assert report["completeness"]["complete"] is True
    assert report["completeness"]["season_mismatch_targets"] == []
    assert report["completeness"]["season_unconfirmed_targets"] == ["epl"]
    assert report["targets"][0]["validation_warnings"] == [
        "details.selectedSeason is missing"
    ]


def test_content_addressed_raw_artifact_is_deterministic_and_reused(tmp_path):
    body = json.dumps(_payload()).encode()
    digest = bench.sha256_bytes(body)

    relative, first_hit = bench.write_raw_artifact(
        tmp_path, bench.SENTINELS[0], body, digest
    )
    same_relative, second_hit = bench.write_raw_artifact(
        tmp_path, bench.SENTINELS[0], body, digest
    )

    assert relative == same_relative == f"raw/epl/{digest}.json.gz"
    assert first_hit is False
    assert second_hit is True
    assert gzip.decompress((tmp_path / relative).read_bytes()) == body


def test_run_benchmark_writes_only_local_artifacts_and_builds_metrics(tmp_path):
    response, encoded, decoded = _json_response(_payload())
    fake = FakeSession(response)

    report = bench.run_benchmark(
        scopes=[bench.SENTINELS[0]],
        artifact_dir=tmp_path,
        label="unit",
        requests_per_minute=0,
        session=fake,
    )

    assert fake.closed is False  # caller-owned sessions are not closed
    assert report["completeness"]["complete"] is True
    assert report["metrics"]["logical_targets"] == 1
    assert report["metrics"]["attempts"] == 1
    assert report["metrics"]["encoded_direct_bytes"] == len(encoded)
    assert report["metrics"]["decoded_direct_bytes"] == len(decoded)
    assert report["metrics"]["encoded_proxy_bytes"] == 0
    assert report["metrics"]["raw_artifact_writes"] == 1
    assert (tmp_path / report["targets"][0]["artifact_path"]).is_file()
    assert "HTTP response bodies on the wire" in report["traffic_measurement"]


def test_canonical_transport_adapter_is_lazy_and_preserves_public_metrics(tmp_path):
    payload = _payload()
    body = json.dumps(payload).encode()

    class FakeTransport:
        def __init__(self):
            self.calls = []
            self.statuses = {}

        def snapshot_stats(self):
            return types.SimpleNamespace(status_counts=dict(self.statuses))

        def fetch_json(self, endpoint, params, *, allow_stale_on_error):
            self.calls.append((endpoint, params, allow_stale_on_error))
            self.statuses["200"] = 1
            return types.SimpleNamespace(
                ok=True,
                status="success",
                json_data=payload,
                body=body,
                url="https://www.fotmob.com/api/data/leagues?id=47&season=2025%2F2026",
                target_key="a" * 64,
                http_status=200,
                attempts=1,
                retries=0,
                encoded_bytes=123,
                decoded_bytes=len(body),
                direct_bytes=123,
                proxy_bytes=0,
                cache_hit=False,
                stale=False,
                etag='"unit"',
                last_modified=None,
                content_hash=fetch_hash,
                raw_uri="file:///tmp/raw.json.gz",
                fetched_at="2026-07-11T00:00:00+00:00",
                error=None,
            )

    fetch_hash = bench.sha256_bytes(body)
    transport = FakeTransport()
    report = bench.run_benchmark(
        scopes=[bench.SENTINELS[0]],
        artifact_dir=tmp_path,
        label="canonical-unit",
        requests_per_minute=0,
        transport_mode="canonical",
        canonical_transport=transport,
    )

    assert transport.calls == [("leagues", {"id": "47", "season": "2025/2026"}, False)]
    assert report["transport"] == "canonical"
    assert report["metrics"]["attempts"] == 1
    assert report["metrics"]["status_counts"] == {"200": 1}
    assert report["metrics"]["encoded_direct_bytes"] == 123
    assert report["metrics"]["decoded_direct_bytes"] == len(body)
    assert report["targets"][0]["target_key"] == "a" * 64
    assert report["targets"][0]["transport_raw_uri"] == "file:///tmp/raw.json.gz"


def test_unexpected_target_failure_is_reported_without_dropping_scope(tmp_path):
    report = bench.run_benchmark(
        scopes=[bench.SENTINELS[0]],
        artifact_dir=tmp_path,
        label="failed-unit",
        requests_per_minute=0,
        session=FakeSession(RuntimeError("adapter bug")),
    )

    assert report["completeness"]["complete"] is False
    assert report["completeness"]["failed_targets"] == ["epl"]
    assert report["metrics"]["attempts"] == 1
    assert report["metrics"]["status_counts"] == {"exception": 1}
    assert report["targets"][0]["validation_errors"] == [
        "unexpected_error: RuntimeError: adapter bug"
    ]


def test_retry_after_accepts_delta_and_http_date():
    now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
    assert bench.parse_retry_after("2.5", now=now) == 2.5
    assert bench.parse_retry_after("Sat, 11 Jul 2026 12:00:03 GMT", now=now) == 3
    assert bench.parse_retry_after("invalid", now=now) is None
