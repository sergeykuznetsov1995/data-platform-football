"""Stage-A gateway characterization (epic #2400, issue #2403).

Pins every battle branch of the Transfermarkt transport gateway BEFORE
the layer-1 extraction, so the rewrite keeps the exact behaviour that
survived production:

* 409 of two kinds on lease close (teardown race vs permanent
  accounting-uncertainty latch),
* pseudo-status-0 transport failures,
* WAF/Cloudflare challenges (202 AWS WAF, 200 HTML, 200 JSON),
* request-permit polling,
* the lease ladders (429 ``concurrency_limited`` backoff and the
  blocked-exit ladder for 403/405/429),
* 405 exiting early under ``max_405_attempts``,
* 404 never becoming an authoritative empty payload.

Bodies marked "recorded" come from the frozen fixture sample
(``tests/fixtures/transfermarkt/stage_a_rewrite``, seed 20261121).
Older, deeper pins for these branches live in
``test_transfermarkt_proxy_transport_v2.py`` and
``test_transfermarkt_traffic.py``; this file keeps one independent,
self-contained pin per branch so adapting those older tests to the
explicit portion context cannot silently drop branch coverage.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from scrapers.transfermarkt import TransfermarktScraper
from scrapers.transfermarkt.client import (
    LeaseConcurrencyLimited,
    ProxyFilterLeaseProvider,
    TransfermarktHttpClient,
)
from scrapers.transfermarkt.models import (
    FetchStatus,
    LeaseTrafficSnapshot,
    ProxyLease,
    SharedTrafficLedger,
)
from scrapers.transfermarkt.raw_store import RawResponseStore

CONTROL_TOKEN = "c" * 32

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures/transfermarkt/stage_a_rewrite"


class _Response:
    def __init__(self, body: bytes, *, status: int = 200, headers=None):
        self.content = body
        self.status_code = status
        self.headers = headers if headers is not None else {
            "Content-Length": str(len(body)),
        }

    @property
    def text(self):
        return self.content.decode("utf-8", errors="replace")


class _TlsClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = 0
        self.closed = False

    def get(self, url, **kwargs):
        self.calls += 1
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def close(self):
        self.closed = True


class _TlsFactory:
    def __init__(self, responses):
        self.responses = list(responses)
        self.clients = []

    def __call__(self, **kwargs):
        client = _TlsClient(self.responses)
        self.clients.append(client)
        return client


class _FakeLeaseProvider:
    def __init__(self, snapshots=()):
        self.snapshots = list(snapshots)
        self.acquired = []
        self.closed = []
        self.permits = []
        self.site_blocks = []
        self.current = LeaseTrafficSnapshot()

    def acquire(self, *, max_bytes, ttl_seconds, metadata):
        lease = ProxyLease(
            lease_id=f"lease-{len(self.acquired) + 1}",
            token=f"token-{len(self.acquired) + 1}",
            proxy_url="http://proxy_filter:8900",
            max_bytes=max_bytes,
            expires_at=9_999_999_999,
        )
        self.acquired.append((lease, ttl_seconds, dict(metadata)))
        self.current = LeaseTrafficSnapshot()
        return lease

    def stats(self, lease):
        if self.snapshots:
            self.current = self.snapshots.pop(0)
        return self.current

    def close(self, lease):
        self.closed.append(lease.lease_id)
        return LeaseTrafficSnapshot(closed=True)

    def acquire_request_permit(self, *, metadata, request_id, **kwargs):
        self.permits.append((dict(metadata), request_id))
        return f"permit-{len(self.permits)}"

    def report_site_block(self, *, status, challenge):
        self.site_blocks.append((status, challenge))

    @staticmethod
    def authenticated_proxy_url(lease):
        return f"http://lease:{lease.token}@proxy_filter:8900"


class _ControlResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class _ControlClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


def _metadata():
    return {
        "dag_id": "dag_ingest_transfermarkt",
        "run_id": "run-1",
        "task_id": "capture_scope",
        "scope": "GB1/2025",
    }


def _recorded_entry(endpoint: str, *, status_code: int) -> dict:
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    for entry in manifest["fixtures"]:
        if entry["endpoint"] != endpoint:
            continue
        capture = json.loads((FIXTURES / entry["capture_file"]).read_text())
        if capture.get("status_code") == status_code:
            return {**entry, "url": capture["url"]}
    raise LookupError(f"no recorded {endpoint} capture with status {status_code}")


def _recorded_body(entry: dict) -> bytes:
    return gzip.decompress((FIXTURES / entry["blob_file"]).read_bytes())


def _client(responses, *, raw_store=None, cache=None, **kwargs):
    provider = kwargs.pop("lease_provider", _FakeLeaseProvider())
    return TransfermarktHttpClient(
        lease_provider=provider,
        traffic_ledger=SharedTrafficLedger(),
        lease_metadata=_metadata(),
        raw_store=raw_store,
        client_factory=_TlsFactory(responses),
        cache=cache,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# 409 of the first kind: close teardown race
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_409_teardown_race_is_retried_to_a_clean_close():
    sleeps = []
    control = _ControlClient([
        _ControlResponse(409, {"code": "lease_close_pending", "close_error": "tunnels draining"}),
        _ControlResponse(409, {"code": "lease_close_pending", "close_error": "tunnels draining"}),
        _ControlResponse(200, {"up_bytes": 10, "down_bytes": 20, "closed": True}),
    ])
    provider = ProxyFilterLeaseProvider(
        "http://proxy_filter:8899",
        control_client=control,
        control_token=CONTROL_TOKEN,
        sleep_fn=sleeps.append,
    )
    lease = ProxyLease(
        lease_id="lease-1", token="t" * 8, proxy_url="http://proxy_filter:8900",
        max_bytes=1000, expires_at=9_999_999_999,
    )

    snapshot = provider.close(lease)

    assert (snapshot.up_bytes, snapshot.down_bytes, snapshot.closed) == (10, 20, True)
    assert sleeps == [0.2, 0.5]
    assert len(control.calls) == 3


# ---------------------------------------------------------------------------
# 409 of the second kind: permanent accounting-uncertainty latch
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_409_uncertainty_latch_keeps_escrow_counters_without_retry():
    sleeps = []
    control = _ControlClient([
        _ControlResponse(409, {
            "code": "lease_close_pending",
            "close_error": "provider byte accounting is uncertain after abort",
            "up_bytes": 100,
            "down_bytes": 900,
        }),
    ])
    provider = ProxyFilterLeaseProvider(
        "http://proxy_filter:8899",
        control_client=control,
        control_token=CONTROL_TOKEN,
        sleep_fn=sleeps.append,
    )
    lease = ProxyLease(
        lease_id="lease-1", token="t" * 8, proxy_url="http://proxy_filter:8900",
        max_bytes=1000, expires_at=9_999_999_999,
    )

    snapshot = provider.close(lease)

    assert (snapshot.up_bytes, snapshot.down_bytes) == (100, 900)
    assert len(control.calls) == 1
    assert sleeps == []


# ---------------------------------------------------------------------------
# pseudo-status-0
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_pseudo_status_zero_is_transport_not_raw_store_failure(tmp_path):
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    client = _client([
        _Response(b"", status=0),
        _Response(b"<html>ok</html>"),
    ], raw_store=raw_store)

    outcome = client.fetch(
        "https://www.transfermarkt.com/liga-de-ascenso/startseite/wettbewerb/CL2B/plus/?saison_id=2025",
        as_json=False,
        label="listing",
    )

    assert outcome.status is FetchStatus.OK
    assert outcome.attempts == 2
    stats = client.get_traffic_stats()
    assert stats["status_counts"] == {"0": 1, "200": 1}
    envelopes = client.get_raw_attempt_records()
    by_kind = {record["outcome_kind"]: record for record in envelopes}
    assert set(by_kind) == {"transport_error", "response"}
    # Envelope records are hash-ordered; attempt ordinals carry the chronology.
    assert by_kind["transport_error"]["attempt"] < by_kind["response"]["attempt"]
    assert by_kind["transport_error"]["error_kind"] == "connection"
    # A pseudo response has no real HTTP status: no capture may be committed.
    assert client.get_raw_capture_records()[0]["status_code"] == 200


# ---------------------------------------------------------------------------
# request-permit polling
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_permit_polling_pends_then_grants_and_consumes_once():
    clock = [100.0]
    permit_id, permit_token = "a" * 64, "p" * 32
    control = _ControlClient([
        _ControlResponse(429, {
            "code": "request_permit_pending", "granted": False,
            "retry_after_seconds": 0.5,
        }),
        _ControlResponse(200, {
            "permit_id": permit_id, "permit_token": permit_token,
            "granted": True,
        }),
        _ControlResponse(200, {
            "permit_id": permit_id, "consumed": True,
        }),
    ])
    provider = ProxyFilterLeaseProvider(
        "http://proxy_filter:8899",
        control_client=control,
        control_token=CONTROL_TOKEN,
        sleep_fn=lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        time_fn=lambda: clock[0],
    )

    granted = provider.acquire_request_permit(
        metadata=_metadata(), request_id="request-1",
    )

    assert granted == permit_id
    assert [call[1].rsplit("/", 1)[-1] for call in control.calls] == [
        "request-permits", "request-permits", "consume",
    ]


# ---------------------------------------------------------------------------
# lease ladders
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_lease_concurrency_ladder_sleeps_then_raises_concurrency_limited():
    sleeps = []
    busy = {"code": "concurrency_limited", "error": "lease concurrency limit reached"}
    control = _ControlClient([_ControlResponse(429, busy)] * 6)
    provider = ProxyFilterLeaseProvider(
        "http://proxy_filter:8899",
        control_client=control,
        control_token=CONTROL_TOKEN,
        sleep_fn=sleeps.append,
    )

    with pytest.raises(LeaseConcurrencyLimited, match="HTTP 429"):
        provider.acquire(
            max_bytes=1000, ttl_seconds=60,
            metadata={**_metadata(), "canonical_url": "https://www.transfermarkt.com/x"},
        )

    assert sleeps == [2.0, 5.0, 10.0, 15.0, 15.0]
    assert len(control.calls) == 6


@pytest.mark.unit
def test_blocked_403_ladder_rotates_exits_up_to_the_blocked_cap():
    blocked = [_Response(b"<html>forbidden</html>", status=403)] * 6
    client = _client(blocked)

    outcome = client.fetch(
        "https://www.transfermarkt.com/primera-nacional/startseite/wettbewerb/ARG2/plus/?saison_id=2025",
        as_json=False,
        label="listing",
    )

    assert outcome.status is FetchStatus.BLOCKED
    assert outcome.attempts == 4  # _MAX_BLOCKED_ATTEMPTS, not the full 8
    provider = client._lease_provider
    assert len(provider.acquired) == 4  # each block rotates to a fresh lease


# ---------------------------------------------------------------------------
# 405=exit with the recorded body
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_recorded_405_listing_url_exits_after_max_405_attempts():
    # The recorded 405 (attempt envelope 46fa7eb5..., seed 20261121) kept its
    # URL but not its body: the sample predates the #2402 warehouse nuke and
    # the capture blob was not among the 44 sampled captures.  The ladder is
    # behaviour, not bytes, so a minimal body drives it here.
    url = ("https://www.transfermarkt.com/prva-liga/startseite/wettbewerb/SL1/"
           "plus/?saison_id=2026")
    client = _client([_Response(b"<html>405</html>", status=405)] * 4)

    outcome = client.fetch(url, as_json=False, label="listing", max_405_attempts=2)

    assert outcome.status is FetchStatus.BLOCKED
    assert outcome.attempts == 2
    assert outcome.status_code == 405
    assert len(client._lease_provider.permits) == 2


# ---------------------------------------------------------------------------
# 404 != empty
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_404_is_not_authoritative_empty_and_is_never_cached():
    cache = {}
    client = _client([_Response(b"<html>gone</html>", status=404)], cache=cache)

    outcome = client.fetch(
        "https://www.transfermarkt.com/ceapi/transferHistory/list/999999999",
        as_json=True,
        label="transfer_events",
        cache_key="https://www.transfermarkt.com/ceapi/transferHistory/list/999999999",
        cache_ttl_seconds=3600.0,
    )

    assert outcome.status is FetchStatus.RETRY_EXHAUSTED
    assert outcome.attempts == 1
    assert outcome.error == "HTTP 404 is not an authoritative empty payload"
    assert cache == {}  # a 404 must never become negative cache


# ---------------------------------------------------------------------------
# WAF / Cloudflare challenges
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cf_html_challenge_on_200_is_blocked_not_schema_error():
    challenge = (
        b"<html><title>Just a moment...</title>"
        b"<script src='/cdn-cgi/challenge-platform/h/b/or.js'></script></html>"
    )
    client = _client([_Response(challenge)] * 2)

    outcome = client.fetch(
        "https://www.transfermarkt.com/some/page", as_json=False, label="listing",
    )

    assert outcome.status is FetchStatus.BLOCKED
    assert outcome.error == "Cloudflare HTML challenge"
    assert outcome.attempts == 2  # bounded ladder: min(attempts_cap, 2)


@pytest.mark.unit
def test_cf_json_challenge_on_200_is_blocked():
    client = _client([
        _Response(b'{"error": "cloudflare challenge session"}'),
    ] * 2)

    outcome = client.fetch(
        "https://www.transfermarkt.com/ceapi/marketValueDevelopment/graph/1",
        as_json=True,
        label="market_value_points",
    )

    assert outcome.status is FetchStatus.BLOCKED
    assert outcome.error == "Cloudflare JSON challenge"


@pytest.mark.unit
def test_aws_waf_202_challenge_is_blocked():
    body = (
        b"<html><script src='https://abc.challenge-runner.awswaf.com/"
        b"abc/challenge.js'></script></html>"
    )
    client = _client([_Response(body, status=202)] * 2)

    outcome = client.fetch(
        "https://www.transfermarkt.com/some/page", as_json=False, label="listing",
    )

    assert outcome.status is FetchStatus.BLOCKED
    assert outcome.error == "HTTP 202 AWS WAF challenge"


# ---------------------------------------------------------------------------
# recorded happy paths through the whole transport
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_recorded_gateway_probe_body_commits_verified_raw_evidence(tmp_path):
    entry = _recorded_entry("gateway_probe", status_code=200)
    body = _recorded_body(entry)
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    client = _client([_Response(body)], raw_store=raw_store)

    outcome = client.fetch(entry["url"], as_json=False, label="gateway_probe")

    assert outcome.status is FetchStatus.OK
    assert outcome.status_code == 200
    assert len(outcome.value.encode("utf-8", errors="replace")) == len(body)
    replayed, record = raw_store.load_capture(outcome.raw_capture_id)
    assert replayed == body
    assert record.endpoint == "gateway_probe"
    assert client.get_traffic_stats()["decoded_response_body_bytes"] == len(body)


@pytest.mark.unit
def test_recorded_squad_body_passes_the_endpoint_validator(tmp_path):
    entry = _recorded_entry("squad", status_code=200)
    body = _recorded_body(entry)
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    client = _client([_Response(body)], raw_store=raw_store)

    outcome = client.fetch(
        entry["url"],
        as_json=False,
        label="squad",
        validator=TransfermarktScraper._endpoint_validator("squad", False),
    )

    assert outcome.status is FetchStatus.OK
    assert outcome.raw_capture_id
    assert client.get_raw_attempt_records()[0]["endpoint"] == "squad"
