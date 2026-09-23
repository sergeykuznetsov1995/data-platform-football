"""Load probe for the SofaScore gateway control channel (#1350)."""

from __future__ import annotations

import importlib.util
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "deploy" / "sofascore" / "gateway_probe_load.py"


def _load():
    spec = importlib.util.spec_from_file_location("gateway_probe_load", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


probe = _load()


def test_percentile_is_nearest_rank():
    values = [float(value) for value in range(1, 101)]
    assert probe.percentile(values, 0.50) == 50.0
    assert probe.percentile(values, 0.95) == 95.0
    assert probe.percentile([7.0], 0.95) == 7.0
    assert probe.percentile([3.0, 1.0, 2.0], 0.5) == 2.0
    assert probe.percentile([], 0.5) is None
    with pytest.raises(ValueError):
        probe.percentile([1.0], 0.0)


def test_summary_counts_timeouts_and_keeps_the_contract_keys():
    summary = probe.summarize(
        [1.0, 2.0, 30.0], errors=1, timeouts=2, status_counts={"200": 3}
    )
    assert summary == {
        "count": 5,
        "p50_ms": 2.0,
        "p95_ms": 30.0,
        "max_ms": 30.0,
        "errors": 1,
        "timeouts": 2,
        "status_counts": {"200": 3},
    }
    empty = probe.summarize([], errors=0, timeouts=0, status_counts={})
    assert empty["p50_ms"] is None and empty["count"] == 0


def test_arguments_are_parsed_and_validated():
    args = probe.parse_args(
        [
            "--base-url",
            "http://127.0.0.1:8899",
            "--seconds",
            "60",
            "--rate",
            "5",
            "--stats-lease-id",
            "abc",
        ]
    )
    assert (args.base_url, args.seconds, args.rate, args.stats_lease_id) == (
        "http://127.0.0.1:8899",
        60.0,
        5.0,
        "abc",
    )
    assert probe.parse_args(["--base-url", "http://gw:8899"]).stats_lease_id is None
    for bad in (
        ["--base-url", "https://gw:8899"],
        ["--base-url", "http://gw:8899", "--rate", "0"],
        ["--seconds", "1"],
    ):
        with pytest.raises(SystemExit):
            probe.parse_args(bad)


def test_main_prints_one_json_report_per_endpoint(monkeypatch, capsys):
    seen: list[tuple[str, str]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            seen.append((self.path, self.headers.get("X-Proxy-Control-Token", "")))
            status = 200 if self.path == "/health" else 401
            body = b"{}"
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("PROXY_FILTER_CONTROL_TOKEN", "t" * 32)
    try:
        assert (
            probe.main(
                [
                    "--base-url",
                    f"http://127.0.0.1:{server.server_address[1]}",
                    "--seconds",
                    "0.2",
                    "--rate",
                    "20",
                    "--stats-lease-id",
                    "lease/1",
                ]
            )
            == 0
        )
    finally:
        server.shutdown()
        server.server_close()
    report = json.loads(capsys.readouterr().out)
    assert set(report["endpoints"]) == {"health", "stats"}
    for name, code in (("health", "200"), ("stats", "401")):
        endpoint = report["endpoints"][name]
        assert set(endpoint) == {
            "count",
            "p50_ms",
            "p95_ms",
            "max_ms",
            "errors",
            "timeouts",
            "status_counts",
        }
        assert endpoint["count"] >= 1 and endpoint["errors"] == 0
        assert set(endpoint["status_counts"]) == {code}
    assert ("/v1/leases/lease%2F1/stats", "t" * 32) in seen
    assert "t" * 32 not in json.dumps(report), "token must never be printed"
