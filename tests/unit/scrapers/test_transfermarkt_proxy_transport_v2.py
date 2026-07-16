"""Native-v2 paid proxy lease, traffic-ledger, and cache contracts."""

from __future__ import annotations

from dataclasses import replace
from urllib.parse import unquote, urlsplit

import pytest

from scrapers.transfermarkt.client import (
    ProxyFilterLeaseProvider,
    TransfermarktHttpClient,
)
from scrapers.transfermarkt.models import (
    HARD_PROVIDER_BYTE_BUDGET,
    SOFT_PROVIDER_BYTE_STOP,
    FetchOutcome,
    FetchStatus,
    LeaseTrafficSnapshot,
    ProxyLease,
    ProxyRequiredError,
    SharedTrafficLedger,
    TrafficBudgetExceeded,
)


CONTROL_TOKEN = "c" * 32


class _Response:
    def __init__(self, body: bytes, *, status: int = 200, payload=None):
        self.content = body
        self.status_code = status
        self.headers = {"Content-Length": str(len(body))}
        self._payload = payload

    @property
    def text(self):
        return self.content.decode("utf-8")

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _TlsClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []
        self.closed = False

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def close(self):
        self.closed = True


class _TlsFactory:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.clients = []

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        client = _TlsClient(self.responses)
        self.clients.append(client)
        return client


class _FakeLeaseProvider:
    def __init__(self, snapshots):
        self.snapshots = list(snapshots)
        self.acquired = []
        self.closed = []
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
        assert lease == self.acquired[-1][0]
        if self.snapshots:
            self.current = self.snapshots.pop(0)
        return self.current

    def close(self, lease):
        self.closed.append(lease.lease_id)
        return replace(self.current, closed=True)

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


@pytest.mark.unit
def test_default_shared_provider_budget_constants_are_exact():
    ledger = SharedTrafficLedger()

    assert HARD_PROVIDER_BYTE_BUDGET == 15_728_640
    assert SOFT_PROVIDER_BYTE_STOP == 14_680_064
    assert ledger.snapshot()["hard_provider_byte_budget"] == 15_728_640
    assert ledger.snapshot()["soft_provider_byte_stop"] == 14_680_064


@pytest.mark.unit
def test_production_lease_adapter_uses_exact_api_and_basic_proxy_auth():
    control = _ControlClient([
        _ControlResponse(201, {
            "id": "abc",
            "token": "secret/token",
            "proxy_url": "http://proxy_filter:8900",
            "max_bytes": 1234,
            "expires_at": 123.0,
        }),
        _ControlResponse(200, {
            "up_bytes": 10,
            "down_bytes": 90,
            "total_bytes": 100,
        }),
        _ControlResponse(200, {
            "up_bytes": 10,
            "down_bytes": 90,
            "total_bytes": 100,
            "closed": True,
        }),
    ])
    provider = ProxyFilterLeaseProvider(
        "http://proxy_filter:8899",
        control_client=control,
        control_token=CONTROL_TOKEN,
    )
    metadata = {
        **_metadata(),
        "canonical_url": "https://www.transfermarkt.us/premier-league/startseite/GB1",
    }

    lease = provider.acquire(max_bytes=1234, ttl_seconds=300, metadata=metadata)
    assert provider.authenticated_proxy_url(lease) == (
        "http://lease:secret%2Ftoken@proxy_filter:8900"
    )
    assert provider.stats(lease).provider_bytes == 100
    assert provider.close(lease).closed is True

    post = control.calls[0]
    assert post[0:2] == ("POST", "http://proxy_filter:8899/v1/leases")
    assert post[2]["json"]["max_bytes"] == 1234
    assert post[2]["json"]["ttl_seconds"] == 300
    # The control plane authenticates the caller on every call; the lease's own
    # bearer token only says which lease a call is about.
    assert post[2]["headers"] == {"X-Proxy-Control-Token": CONTROL_TOKEN}
    assert control.calls[1][2]["headers"] == {
        "X-Proxy-Control-Token": CONTROL_TOKEN,
        "Authorization": "Bearer secret/token",
    }
    assert control.calls[2][0] == "DELETE"


@pytest.mark.unit
def test_a_lease_is_refused_before_any_call_when_the_control_token_is_absent(
    monkeypatch,
):
    for name in (
        "TM_PROXY_CONTROL_TOKEN",
        "PROXY_FILTER_CONTROL_TOKEN",
        "SOFASCORE_PROXY_CONTROL_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    control = _ControlClient([])

    with pytest.raises(ProxyRequiredError, match="TM_PROXY_CONTROL_TOKEN"):
        ProxyFilterLeaseProvider(
            "http://proxy_filter:8899", control_client=control,
        )

    assert control.calls == []


@pytest.mark.unit
def test_default_tls_transport_resolves_docker_dns_and_keeps_basic_auth(
    monkeypatch,
):
    resolved = []

    def resolve(host):
        resolved.append(host)
        return "172.31.0.7"

    monkeypatch.setattr(
        "scrapers.transfermarkt.client.socket.gethostbyname", resolve,
    )
    client = TransfermarktHttpClient(
        proxy="http://lease:secret%2Ftoken@proxy_filter:8900",
    )

    transport = client._new_tls_client(
        "http://lease:secret%2Ftoken@proxy_filter:8900",
    )
    try:
        assert resolved == ["proxy_filter"]
        assert transport.proxy == (
            "http://lease:secret%2Ftoken@172.31.0.7:8900"
        )
        parsed = urlsplit(transport.proxy)
        assert parsed.username == "lease"
        assert unquote(parsed.password or "") == "secret/token"

        # Exercise the exact parser that rejected ``proxy_filter`` in
        # production.  The adapted URL must be accepted without any I/O.
        import tls_requests

        assert tls_requests.Proxy(transport.proxy).url == transport.proxy
    finally:
        transport.close()


@pytest.mark.unit
def test_default_tls_transport_dns_failure_is_fail_closed_and_redacted(
    monkeypatch,
):
    def fail_resolution(_host):
        raise OSError("not resolvable")

    monkeypatch.setattr(
        "scrapers.transfermarkt.client.socket.gethostbyname", fail_resolution,
    )
    client = TransfermarktHttpClient(
        proxy="http://lease:top-secret@proxy_filter:8900",
    )

    with pytest.raises(ProxyRequiredError, match="metered proxy DNS") as caught:
        client._new_tls_client(
            "http://lease:top-secret@proxy_filter:8900",
        )

    assert "top-secret" not in str(caught.value)


@pytest.mark.unit
def test_sticky_lease_measures_directional_traffic_per_entity():
    provider = _FakeLeaseProvider([
        LeaseTrafficSnapshot(up_bytes=100, down_bytes=900),
        LeaseTrafficSnapshot(up_bytes=180, down_bytes=1_620),
    ])
    factory = _TlsFactory([
        _Response(b"first"),
        _Response(b"second"),
    ])
    ledger = SharedTrafficLedger()
    client = TransfermarktHttpClient(
        lease_provider=provider,
        traffic_ledger=ledger,
        lease_metadata=_metadata(),
        client_factory=factory,
    )

    assert client.fetch(
        "https://www.transfermarkt.us/a", as_json=False, label="squads",
    ).status is FetchStatus.OK
    assert client.fetch(
        "https://www.transfermarkt.us/b", as_json=False, label="squads",
    ).status is FetchStatus.OK

    assert len(provider.acquired) == 1
    assert len(factory.calls) == 1
    assert factory.calls[0]["proxy"].startswith("http://lease:token-1@")
    stats = client.get_traffic_stats()
    assert stats["decoded_response_body_bytes"] == 11
    assert stats["wire_response_bytes"] == 1_620
    assert stats["provider_up_bytes"] == 180
    assert stats["provider_down_bytes"] == 1_620
    assert stats["provider_metered_bytes"] == 1_800
    assert stats["requests"] == 2
    assert stats["by_label"]["squads"]["provider_metered_bytes"] == 1_800
    assert stats["by_label"]["squads"]["duration_seconds"] >= 0
    assert ledger.snapshot()["by_entity"]["squads"]["requests"] == 2


@pytest.mark.unit
def test_shared_soft_stop_blocks_next_client_before_paid_io():
    ledger = SharedTrafficLedger(hard_provider_bytes=100, soft_provider_bytes=80)
    first_provider = _FakeLeaseProvider([
        LeaseTrafficSnapshot(up_bytes=10, down_bytes=80),
    ])
    first_factory = _TlsFactory([_Response(b"ok")])
    first = TransfermarktHttpClient(
        lease_provider=first_provider,
        traffic_ledger=ledger,
        lease_metadata=_metadata(),
        client_factory=first_factory,
    )
    assert first.fetch(
        "https://www.transfermarkt.us/a", as_json=False,
    ).status is FetchStatus.OK

    second_provider = _FakeLeaseProvider([])
    second_factory = _TlsFactory([_Response(b"must-not-run")])
    second = TransfermarktHttpClient(
        lease_provider=second_provider,
        traffic_ledger=ledger,
        lease_metadata=_metadata(),
        client_factory=second_factory,
    )
    with pytest.raises(TrafficBudgetExceeded, match="soft byte stop"):
        second.fetch("https://www.transfermarkt.us/b", as_json=False)

    assert second_provider.acquired == []
    assert second_factory.calls == []
    assert ledger.snapshot()["provider_metered_bytes"] == 90
    assert ledger.snapshot()["soft_stop_reached"] is True


@pytest.mark.unit
def test_shared_retry_cap_blocks_paid_retry_n_plus_one_before_io():
    provider = _FakeLeaseProvider([
        LeaseTrafficSnapshot(up_bytes=10, down_bytes=40),
        LeaseTrafficSnapshot(up_bytes=20, down_bytes=80),
        LeaseTrafficSnapshot(up_bytes=30, down_bytes=120),
    ])
    factory = _TlsFactory([
        _Response(b"blocked-1", status=403),
        _Response(b"blocked-2", status=403),
        _Response(b"blocked-3", status=403),
        _Response(b"must-not-run", status=200),
    ])
    ledger = SharedTrafficLedger(retry_limit=1)
    client = TransfermarktHttpClient(
        lease_provider=provider,
        traffic_ledger=ledger,
        retry_budget=1,
        lease_metadata=_metadata(),
        client_factory=factory,
        sleep_fn=lambda _: None,
    )

    # max_attempts is what bounds this fetch — a block is now worth more than
    # one alternate exit, so the ladder must be capped here to spend exactly
    # the one paid retry the ledger allows.
    first = client.fetch(
        "https://www.transfermarkt.us/a", as_json=False, max_attempts=2,
    )
    assert first.status is FetchStatus.BLOCKED
    with pytest.raises(TrafficBudgetExceeded, match="paid retry budget"):
        client.fetch(
            "https://www.transfermarkt.us/b", as_json=False, max_attempts=2,
        )

    assert sum(len(item.calls) for item in factory.clients) == 3
    assert len(provider.acquired) == 3
    assert ledger.snapshot()["retries"] == 1
    assert ledger.snapshot()["remaining_retries"] == 0


@pytest.mark.unit
def test_blocked_lease_rotates_once_and_never_direct_falls_back():
    provider = _FakeLeaseProvider([
        LeaseTrafficSnapshot(up_bytes=10, down_bytes=40),
        LeaseTrafficSnapshot(up_bytes=5, down_bytes=25),
    ])
    factory = _TlsFactory([
        _Response(b"blocked", status=403),
        _Response(b"ok"),
    ])
    client = TransfermarktHttpClient(
        lease_provider=provider,
        lease_metadata=_metadata(),
        client_factory=factory,
        sleep_fn=lambda _: None,
    )

    outcome = client.fetch("https://www.transfermarkt.us/a", as_json=False)

    assert outcome.status is FetchStatus.OK
    assert outcome.attempts == 2
    assert len(provider.acquired) == 2
    assert provider.closed == ["lease-1"]
    assert len(factory.calls) == 2
    assert all("lease:" in item["proxy"] for item in factory.calls)
    assert client.get_traffic_stats()["retries"] == 1


@pytest.mark.unit
def test_integrity_checked_cache_hit_has_zero_network_requests():
    cache = {}
    factory = _TlsFactory([_Response(b"payload")])
    client = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        cache=cache,
        client_factory=factory,
    )

    first = client.fetch(
        "https://www.transfermarkt.us/a",
        as_json=False,
        label="coach_profiles",
        cache_key="coach:1",
        cache_ttl_seconds=60,
    )
    second = client.fetch(
        "https://www.transfermarkt.us/a",
        as_json=False,
        label="coach_profiles",
        cache_key="coach:1",
        cache_ttl_seconds=60,
    )

    assert first.cache_hit is False
    assert second.cache_hit is True
    assert second.attempts == 0
    assert len(factory.clients[0].calls) == 1
    stats = client.get_traffic_stats()
    assert stats["requests"] == 1
    assert stats["cache_hits"] == 1
    assert stats["cache_hit_rate"] == 0.5
    assert stats["by_label"]["coach_profiles"]["cache_hit_rate"] == 0.5


@pytest.mark.unit
def test_checkpoint_migrates_valid_empty_and_rejects_hash_tampering():
    legacy = FetchOutcome(
        status=FetchStatus.VALID_EMPTY,
        value=[],
        payload_hash=None,
    ).as_checkpoint()

    restored = FetchOutcome.from_checkpoint(legacy)
    assert restored.status is FetchStatus.AUTHORITATIVE_EMPTY
    assert restored.is_success is True
    assert FetchOutcome(
        status=FetchStatus.NOT_APPLICABLE,
        value=None,
    ).is_success is True

    corrupt = dict(legacy)
    corrupt["value"] = ["unexpected"]
    with pytest.raises(ValueError, match="hash mismatch"):
        FetchOutcome.from_checkpoint(corrupt)


@pytest.mark.unit
def test_transfermarkt_refuses_another_sources_control_secret(
    monkeypatch,
):
    # A common bearer must not let an arbitrary worker impersonate an
    # allowlisted Transfermarkt DAG.
    monkeypatch.delenv("TM_PROXY_CONTROL_TOKEN", raising=False)
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", CONTROL_TOKEN)

    with pytest.raises(ProxyRequiredError, match="TM_PROXY_CONTROL_TOKEN"):
        ProxyFilterLeaseProvider(
            "http://proxy_filter:8899", control_client=_ControlClient([]),
        )


@pytest.mark.unit
def test_a_page_paid_for_by_one_cycle_is_not_paid_for_again():
    # A league can exceed one cycle's byte cap, so the next cycle must finish it
    # from the pages the previous one already bought.
    cache: dict = {}
    url = "https://www.transfermarkt.com/x/kader/verein/4128"

    first = TransfermarktHttpClient(
        lease_provider=_FakeLeaseProvider([
            LeaseTrafficSnapshot(up_bytes=100, down_bytes=900),
        ]),
        traffic_ledger=SharedTrafficLedger(),
        lease_metadata=_metadata(),
        client_factory=_TlsFactory([_Response(b"<html/>")]),
        cache=cache,
    )
    paid = first.fetch(url, as_json=False, cache_key=url, cache_ttl_seconds=3600)
    assert paid.status is FetchStatus.OK
    assert cache

    # The next cycle: a transport with no responses left would fail any real
    # request, and the lease provider would refuse to hand out another lease.
    second = TransfermarktHttpClient(
        lease_provider=_FakeLeaseProvider([]),
        traffic_ledger=SharedTrafficLedger(),
        lease_metadata=_metadata(),
        client_factory=_TlsFactory([]),
        cache=cache,
    )
    reused = second.fetch(url, as_json=False, cache_key=url, cache_ttl_seconds=3600)

    assert reused.status is FetchStatus.OK
    stats = second.get_traffic_stats()
    assert stats["cache_hits"] == 1
    assert stats["provider_metered_bytes"] == 0
