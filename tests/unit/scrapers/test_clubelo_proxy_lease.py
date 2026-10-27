"""Offline control-plane contract; no sockets or real token files."""

import pytest
import requests

from scrapers.clubelo.proxy_lease import ClubEloLease


@pytest.fixture
def control(monkeypatch):
    calls = []

    class Response:
        status_code = 201
        data = {"id": "lease-id", "token": "lease-token",
                "proxy_url": "http://clubelo_proxy_filter:8900"}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def json(self):
            return self.data

        def raise_for_status(self):
            pass

    class Session:
        def __init__(self):
            self.headers = {}

        def post(self, url, **kwargs):
            calls.append(("post", url, kwargs))
            return Response()

        def delete(self, url, **kwargs):
            calls.append(("delete", url, kwargs))
            return Response()

        def close(self):
            calls.append(("close",))

    monkeypatch.setattr(requests, "Session", Session)
    monkeypatch.setenv("CLUBELO_PROXY_CONTROL_TOKEN", "t" * 32)
    monkeypatch.delenv("CLUBELO_PROXY_CONTROL_URL", raising=False)
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag_ingest_clubelo")
    monkeypatch.delenv("AIRFLOW_CTX_RUN_ID", raising=False)
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "scheduled__real_run")
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "scrape_daily")
    return calls, Response


def test_acquire_and_release_contract(control):
    calls, _ = control
    lease = ClubEloLease().acquire()
    assert lease.session.trust_env is False
    assert lease.proxy_url == "http://lease:lease-token@clubelo_proxy_filter:8900"
    lease.close()
    assert calls[0][0:2] == ("post", "http://clubelo_proxy_filter:8899/v1/leases")
    assert calls[0][2]["json"] == {
        "source": "clubelo", "dag_id": "dag_ingest_clubelo",
        "run_id": "scheduled__real_run", "task_id": "scrape_daily",
        "canonical_url": "https://clubelo.com/Ranking", "max_bytes": 33554432,
        "ttl_seconds": 3600,
    }
    assert calls[0][2]["timeout"] == 15
    assert calls[0][2]["allow_redirects"] is False
    assert calls[1][0:2] == ("delete", "http://clubelo_proxy_filter:8899/v1/leases/lease-id")
    assert calls[1][2]["headers"] == {"Authorization": "Bearer lease-token"}
    lease.close()
    assert sum(call[0] == "delete" for call in calls) == 1


def test_token_file_fallback(control, monkeypatch, tmp_path):
    monkeypatch.delenv("CLUBELO_PROXY_CONTROL_TOKEN")
    token_file = tmp_path / "token"
    token_file.write_text("f" * 32 + "\n")
    monkeypatch.setenv("CLUBELO_PROXY_CONTROL_TOKEN_FILE", str(token_file))
    lease = ClubEloLease().acquire()
    assert lease.session.headers["X-Proxy-Control-Token"] == "f" * 32
    lease.close()


def test_missing_token_has_clear_reason_without_network(control, monkeypatch, tmp_path):
    calls, _ = control
    monkeypatch.delenv("CLUBELO_PROXY_CONTROL_TOKEN")
    monkeypatch.setenv("CLUBELO_PROXY_CONTROL_TOKEN_FILE", str(tmp_path / "missing"))
    with pytest.raises(ValueError, match="control token is unavailable"):
        ClubEloLease().acquire()
    assert calls == []


def test_missing_real_context_fails_before_network(control, monkeypatch):
    calls, _ = control
    monkeypatch.delenv("AIRFLOW_CTX_DAG_RUN_ID")
    monkeypatch.setenv("AIRFLOW_CTX_RUN_ID", "legacy__invalid")
    with pytest.raises(ValueError, match="real Airflow"):
        ClubEloLease().acquire()
    assert calls == []


def test_history_requests_full_session_budget(control, monkeypatch):
    calls, _ = control
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "scrape_history")
    lease = ClubEloLease().acquire()
    assert calls[0][2]["json"]["max_bytes"] == 134217728
    lease.close()


def test_refusal_is_not_retried(control):
    calls, Response = control
    Response.status_code = 429
    with pytest.raises(ValueError, match="refused"):
        ClubEloLease().acquire()
    assert len(calls) == 1


def test_bad_proxy_response_retains_identity_for_cleanup(control):
    calls, Response = control
    Response.data = {**Response.data, "proxy_url": "https://invalid"}
    lease = ClubEloLease()
    with pytest.raises(ValueError, match="invalid proxy"):
        lease.acquire()
    lease.close()
    assert calls[1][0] == "delete"
