from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]


def _load_script(name):
    path = REPO_ROOT / "scripts" / "research" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


fetch = _load_script("bench_fotmob_fetch")
replay = _load_script("bench_fotmob_replay")


def _payload():
    return {
        "details": {"id": 47, "selectedSeason": "2025/2026"},
        "fixtures": {"allMatches": [{"id": 1}, {"id": 2}]},
        "table": [{"data": {"table": {"all": [{"id": 10}]}}}],
    }


def _write_fetch_report(tmp_path):
    body = json.dumps(_payload(), separators=(",", ":")).encode()
    body_hash = fetch.sha256_bytes(body)
    relative, _ = fetch.write_raw_artifact(
        tmp_path, fetch.SENTINELS[0], body, body_hash
    )
    payload_hash = fetch.sha256_bytes(fetch.canonical_json_bytes(_payload()))
    report = {
        "artifact_dir": str(tmp_path),
        "targets": [
            {
                "key": "epl",
                "competition_id": 47,
                "source_season_key": "2025/2026",
                "artifact_path": relative,
                "decoded_body_sha256": body_hash,
                "payload_sha256": payload_hash,
            }
        ],
    }
    report_path = tmp_path / "fetch-report.json"
    fetch.atomic_write_json(report_path, report)
    return report_path, body


def test_load_corpus_verifies_body_and_payload_hashes(tmp_path):
    report_path, body = _write_fetch_report(tmp_path)

    corpus = replay.load_corpus(report_path)

    assert len(corpus) == 1
    assert corpus[0].key == "epl"
    assert corpus[0].decoded_body == body
    assert corpus[0].competition_id == 47


def test_load_corpus_refuses_corrupt_or_missing_artifact(tmp_path):
    report_path, _ = _write_fetch_report(tmp_path)
    report = json.loads(report_path.read_text())
    artifact = tmp_path / report["targets"][0]["artifact_path"]
    artifact.write_bytes(b"not-gzip")

    with pytest.raises(ValueError, match="corrupt"):
        replay.load_corpus(report_path)

    report["targets"][0]["artifact_path"] = "raw/epl/missing.json.gz"
    fetch.atomic_write_json(report_path, report)
    with pytest.raises(ValueError, match="missing"):
        replay.load_corpus(report_path)


def test_load_corpus_refuses_fetch_target_marked_failed(tmp_path):
    report_path, _ = _write_fetch_report(tmp_path)
    report = json.loads(report_path.read_text())
    report["targets"][0]["ok"] = False
    fetch.atomic_write_json(report_path, report)

    with pytest.raises(ValueError, match=r"failed=\['epl'\]"):
        replay.load_corpus(report_path)


def test_default_five_iteration_replay_reports_p50_p95_and_zero_network(tmp_path):
    report_path, _ = _write_fetch_report(tmp_path)
    corpus = replay.load_corpus(report_path)
    # Two clock reads per iteration -> durations 0.1, 0.2, ..., 0.5.
    ticks = iter([0, 0.1, 1, 1.2, 2, 2.3, 3, 3.4, 4, 4.5])

    report = replay.run_replay(
        corpus,
        parser=replay.standalone_parser(),
        iterations=5,
        clock=lambda: next(ticks),
    )

    assert report["iterations"] == 5
    assert report["payloads"] == 1
    assert report["metrics"]["p50_total_seconds"] == pytest.approx(0.3)
    assert report["metrics"]["p95_total_seconds"] == pytest.approx(0.5)
    assert report["metrics"]["network_attempts"] == 0
    assert report["metrics"]["encoded_direct_bytes"] == 0
    assert report["metrics"]["encoded_proxy_bytes"] == 0
    assert report["metrics"]["row_counts"]["matches_unique"] == 2
    hashes = {item["result_sha256"] for item in report["iteration_details"]}
    assert hashes == {report["deterministic_result_sha256"]}


def test_explicit_parser_is_loaded_lazily_and_collection_rows_are_hashed(tmp_path):
    report_path, _ = _write_fetch_report(tmp_path)
    corpus = replay.load_corpus(report_path)
    module = types.ModuleType("unit_fotmob_parser")

    def parse(payload, scope=None):
        assert scope is None
        return types.SimpleNamespace(
            matches=tuple(payload["fixtures"]["allMatches"]),
            standings=({"id": 10},),
        )

    module.parse = parse
    sys.modules[module.__name__] = module
    try:
        parser = replay.load_parser("unit_fotmob_parser:parse")
        report = replay.run_replay(corpus, parser=parser, iterations=1)
    finally:
        sys.modules.pop(module.__name__, None)

    target = report["targets"]["epl"]
    assert report["parser"] == "unit_fotmob_parser:parse"
    assert target["row_counts"] == {"matches": 2, "standings": 1}
    assert set(target["collection_hashes"]) == {"matches", "standings"}


def test_nearest_rank_uses_observed_values():
    values = [0.5, 0.1, 0.4, 0.2, 0.3]
    assert replay.nearest_rank(values, 0.5) == 0.3
    assert replay.nearest_rank(values, 0.95) == 0.5
