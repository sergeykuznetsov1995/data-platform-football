"""Unit tests for residential-proxy traffic reporting (issue #789).

Covers the passive aggregation helpers in ``dags/utils/proxy_traffic.py`` and the
``report_proxy_traffic`` FBref callable: summing per-task byte counters into one
run-level summary, per-domain rollup, and never failing the DAG.
"""

import importlib
import json
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def write_fbref_traffic(tmp_path):
    """Factory writing /tmp-style fbref_traffic_<label>.json into a tmp dir."""
    def _write(label: str, payload: dict) -> None:
        (tmp_path / f"fbref_traffic_{label}.json").write_text(json.dumps(payload))
    return _write


@pytest.fixture
def fbref_glob(tmp_path):
    return str(tmp_path / "fbref_traffic_*.json")


class TestHostOf:
    @pytest.mark.unit
    def test_host_from_host_path(self):
        from dags.utils.proxy_traffic import _host_of

        assert _host_of("fbref.com/en/comps/9") == "fbref.com"

    @pytest.mark.unit
    def test_host_from_full_url(self):
        from dags.utils.proxy_traffic import _host_of

        assert _host_of("https://cdn.fbref.com/a/b.js") == "cdn.fbref.com"

    @pytest.mark.unit
    def test_empty(self):
        from dags.utils.proxy_traffic import _host_of

        assert _host_of("") == ""


class TestSummarizeFbrefTraffic:
    @pytest.mark.unit
    def test_sums_cdp_and_http_mb_across_tasks(self, write_fbref_traffic, fbref_glob):
        from dags.utils.proxy_traffic import summarize_fbref_traffic

        write_fbref_traffic("player_stats", {
            "real_proxy_mb": 2.74, "http_mb_downloaded": 1.72,
            "top_traffic_urls": [{"url": "fbref.com/en", "mb": 3.1}],
        })
        write_fbref_traffic("match_schedule", {
            "real_proxy_mb": 0.5, "http_mb_downloaded": 0.0,
            "top_traffic_urls": [{"url": "cdn.fbref.com/x.js", "mb": 0.4}],
        })

        summary = summarize_fbref_traffic(glob_pattern=fbref_glob)

        # 2.74 + 1.72 + 0.5 + 0.0 = 4.96
        assert summary["source"] == "fbref"
        assert summary["total_mb"] == pytest.approx(4.96)
        assert summary["files_read"] == 2
        hosts = {d["host"]: d["mb"] for d in summary["top_domains"]}
        assert hosts == {
            "fbref.com": pytest.approx(3.1),
            "cdn.fbref.com": pytest.approx(0.4),
        }

    @pytest.mark.unit
    def test_falls_back_to_bytes_when_mb_missing(self, write_fbref_traffic, fbref_glob):
        from dags.utils.proxy_traffic import summarize_fbref_traffic

        write_fbref_traffic("team_stats", {
            "real_proxy_mb": 1.0,
            "top_traffic_urls": [{"url": "fbref.com", "bytes": 1024 * 1024}],
        })

        summary = summarize_fbref_traffic(glob_pattern=fbref_glob)

        assert summary["top_domains"][0]["mb"] == pytest.approx(1.0)

    @pytest.mark.unit
    def test_no_files_is_zero_not_error(self, fbref_glob):
        from dags.utils.proxy_traffic import summarize_fbref_traffic

        summary = summarize_fbref_traffic(glob_pattern=fbref_glob)

        assert summary == {
            "source": "fbref", "total_mb": 0.0, "top_domains": [], "files_read": 0,
        }

    @pytest.mark.unit
    def test_skips_corrupt_json(self, tmp_path, write_fbref_traffic, fbref_glob):
        from dags.utils.proxy_traffic import summarize_fbref_traffic

        write_fbref_traffic("good", {"real_proxy_mb": 2.0})
        (tmp_path / "fbref_traffic_bad.json").write_text("{not json")

        summary = summarize_fbref_traffic(glob_pattern=fbref_glob)

        assert summary["total_mb"] == pytest.approx(2.0)
        assert summary["files_read"] == 1


class TestSummarizeResultTraffic:
    @pytest.mark.unit
    def test_reads_tls_proxy_response_mb(self):
        from dags.utils.proxy_traffic import summarize_result_traffic

        summary = summarize_result_traffic("transfermarkt", {
            "proxy_response_mb": 12.3,
            "top_traffic_urls": [{"url": "transfermarkt.us", "mb": 12.3}],
        })

        assert summary["source"] == "transfermarkt"
        assert summary["total_mb"] == pytest.approx(12.3)
        assert summary["top_domains"][0]["host"] == "transfermarkt.us"

    @pytest.mark.unit
    def test_reads_flaresolverr_fs_response_mb(self):
        from dags.utils.proxy_traffic import summarize_result_traffic

        summary = summarize_result_traffic("sofifa", {"fs_response_mb": 0.0})

        assert summary["total_mb"] == 0.0
        assert summary["top_domains"] == []


class TestLogTrafficSummary:
    @pytest.mark.unit
    def test_emits_proxy_traffic_line(self, caplog):
        import logging

        from dags.utils.proxy_traffic import log_traffic_summary

        with caplog.at_level(logging.INFO):
            log_traffic_summary({
                "source": "fbref", "total_mb": 4.96,
                "top_domains": [{"host": "fbref.com", "mb": 3.1}],
            })

        assert "PROXY_TRAFFIC source=fbref" in caplog.text
        assert "fbref.com 3.1 MB" in caplog.text


class TestReportProxyTrafficCallable:
    """The FBref callable must push the run total to XCom and never raise."""

    @pytest.mark.unit
    def test_pushes_total_to_xcom(self, monkeypatch):
        from dags.utils import fbref_callbacks

        # Patch the module the callable imports from (utils.proxy_traffic),
        # which is the same object resolved by its in-function import.
        pt = importlib.import_module("utils.proxy_traffic")
        monkeypatch.setattr(
            pt, "summarize_fbref_traffic",
            lambda: {"source": "fbref", "total_mb": 3.0, "top_domains": []},
        )
        ti = MagicMock()

        result = fbref_callbacks.report_proxy_traffic(ti=ti)

        assert result["total_mb"] == pytest.approx(3.0)
        ti.xcom_push.assert_called_with(key="proxy_total_mb", value=3.0)

    @pytest.mark.unit
    def test_never_raises_on_failure(self, monkeypatch):
        from dags.utils import fbref_callbacks

        def _boom():
            raise RuntimeError("disk gone")

        pt = importlib.import_module("utils.proxy_traffic")
        monkeypatch.setattr(pt, "summarize_fbref_traffic", _boom)

        # Must swallow the error and return a sentinel, not raise.
        result = fbref_callbacks.report_proxy_traffic(ti=MagicMock())

        assert result == {"source": "fbref", "total_mb": None}
