"""Small, single-attempt client for the dedicated ClubElo gateway."""

import os
from pathlib import Path
from urllib.parse import quote, urlsplit

import requests


class ClubEloLease:
    def __init__(self):
        self.session = requests.Session()
        self.session.trust_env = False
        self.url = os.environ.get(
            "CLUBELO_PROXY_CONTROL_URL", "http://clubelo_proxy_filter:8899"
        ).rstrip("/")
        self.lease_id = None
        self.token = None
        self.proxy_url = None

    def acquire(self):
        control_token = os.environ.get("CLUBELO_PROXY_CONTROL_TOKEN", "").strip()
        if not control_token:
            try:
                control_token = Path(os.environ.get(
                    "CLUBELO_PROXY_CONTROL_TOKEN_FILE",
                    "/opt/airflow/configs/proxy_filter/clubelo_control_token",
                )).read_text().strip()
            except OSError:
                raise ValueError("ClubElo proxy control token is unavailable") from None
        if len(control_token) < 32:
            raise ValueError("ClubElo proxy control token must contain at least 32 characters")
        self.session.headers["X-Proxy-Control-Token"] = control_token
        context = {name: os.environ.get("AIRFLOW_CTX_" + name.upper(), "")
                   for name in ("dag_id", "run_id", "task_id")}
        if (context["dag_id"] != "dag_ingest_clubelo" or not context["run_id"]
                or context["task_id"] not in {"scrape_daily", "scrape_history", "check_watch"}):
            raise ValueError("ClubElo lease requires the real Airflow task context")
        history = context["task_id"] == "scrape_history"
        with self.session.post(
            self.url + "/v1/leases",
            json={**context, "source": "clubelo", "canonical_url": "https://clubelo.com/Ranking",
                  "max_bytes": int(os.environ.get(
                      "CLUBELO_PROXY_HISTORY_MAX_BYTES" if history else "CLUBELO_PROXY_MAX_BYTES",
                      str(128 * 1024 * 1024) if history else str(32 * 1024 * 1024))),
                  "ttl_seconds": int(os.environ.get("CLUBELO_PROXY_TTL_SECONDS", "3600"))},
            timeout=15, allow_redirects=False,
        ) as response:
            if response.status_code != 201:
                raise ValueError("ClubElo gateway refused the lease")
            data = response.json()
        # Retain the identity for cleanup even if the proxy URL is malformed.
        self.lease_id, self.token = data.get("id"), data.get("token")
        if not all(isinstance(value, str) and value for value in (self.lease_id, self.token)):
            raise ValueError("ClubElo gateway returned an invalid lease")
        proxy = urlsplit(data.get("proxy_url", ""))
        if (proxy.scheme != "http" or not proxy.hostname or not proxy.port
                or proxy.username or proxy.password or proxy.path not in ("", "/")
                or proxy.query or proxy.fragment):
            raise ValueError("ClubElo gateway returned an invalid proxy URL")
        self.proxy_url = f"http://lease:{quote(self.token, safe='')}@{proxy.netloc}"
        return self

    def close(self):
        try:
            if self.lease_id and self.token:
                lease_id, self.lease_id = self.lease_id, None
                with self.session.delete(
                    self.url + "/v1/leases/" + quote(lease_id, safe=""),
                    headers={"Authorization": "Bearer " + self.token},
                    timeout=15, allow_redirects=False,
                ) as response:
                    response.raise_for_status()
        finally:
            self.session.close()
