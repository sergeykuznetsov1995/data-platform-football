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
    PROVIDER_GRANT_ENV_VAR,
    PROVIDER_GRANT_FLOOR_BYTES,
    PROVIDER_GRANT_SOFT_MARGIN_BYTES,
    SCOPE_HARD_PROVIDER_BYTE_CAP,
    SCOPE_SOFT_PROVIDER_BYTE_STOP,
    SOFT_PROVIDER_BYTE_STOP,
    FetchOutcome,
    FetchStatus,
    LeaseTrafficSnapshot,
    ProxyLease,
    ProxyRequiredError,
    SharedTrafficLedger,
    TrafficBudgetExceeded,
    TrafficMeterError,
)
from scrapers.transfermarkt.raw_store import RawResponseStore, RawStoreError


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
        self.permits = []

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

    def acquire_request_permit(self, *, metadata, request_id):
        self.permits.append((dict(metadata), request_id))
        return f"permit-{len(self.permits)}"

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

    # Production per-scope pair: the default ledger of one exact scope cycle.
    assert SCOPE_HARD_PROVIDER_BYTE_CAP == 25_165_824
    assert SCOPE_SOFT_PROVIDER_BYTE_STOP == 23_068_672
    assert ledger.snapshot()["hard_provider_byte_budget"] == 25_165_824
    assert ledger.snapshot()["soft_provider_byte_stop"] == 23_068_672
    # The discovery contour keeps its own explicitly-pinned 15/14 MiB pair.
    assert HARD_PROVIDER_BYTE_BUDGET == 15_728_640
    assert SOFT_PROVIDER_BYTE_STOP == 14_680_064


@pytest.mark.unit
def test_provider_grant_and_entity_reserve_literals_are_pinned():
    from scrapers.transfermarkt.models import PRODUCTION_ENTITY_BUDGETS

    # These are paid-traffic literals: a silent edit changes what production
    # is allowed to spend, so they are pinned by value, not by derivation.
    assert PROVIDER_GRANT_FLOOR_BYTES == 65_536
    assert PROVIDER_GRANT_SOFT_MARGIN_BYTES == 1_048_576
    assert {
        name: budget["provider_reserve_bytes"]
        for name, budget in PRODUCTION_ENTITY_BUDGETS.items()
    } == {
        "players": 10_485_760,
        "market_value_history": 6_291_456,
        "transfers": 8_388_608,
        "coaches": 8_388_608,
    }


@pytest.mark.unit
def test_provider_grant_sizes_the_metered_client_ledger(monkeypatch):
    grant = 10 * 1024 * 1024
    monkeypatch.setenv(PROVIDER_GRANT_ENV_VAR, str(grant))
    client = TransfermarktHttpClient(
        lease_provider=_FakeLeaseProvider([]),
        lease_metadata=_metadata(),
        client_factory=_TlsFactory([]),
    )

    snapshot = client._traffic_ledger.snapshot()
    assert snapshot["hard_provider_byte_budget"] == grant
    assert snapshot["soft_provider_byte_stop"] == (
        grant - PROVIDER_GRANT_SOFT_MARGIN_BYTES
    )


@pytest.mark.unit
def test_provider_grant_never_exceeds_the_scope_cap(monkeypatch):
    monkeypatch.setenv(
        PROVIDER_GRANT_ENV_VAR, str(SCOPE_HARD_PROVIDER_BYTE_CAP * 4),
    )
    client = TransfermarktHttpClient(
        lease_provider=_FakeLeaseProvider([]),
        lease_metadata=_metadata(),
        client_factory=_TlsFactory([]),
    )

    snapshot = client._traffic_ledger.snapshot()
    assert snapshot["hard_provider_byte_budget"] == SCOPE_HARD_PROVIDER_BYTE_CAP


@pytest.mark.unit
def test_provider_grant_below_the_floor_is_refused_before_any_io(monkeypatch):
    monkeypatch.setenv(
        PROVIDER_GRANT_ENV_VAR, str(PROVIDER_GRANT_FLOOR_BYTES - 1),
    )
    provider = _FakeLeaseProvider([])
    factory = _TlsFactory([])

    with pytest.raises(TrafficMeterError, match="floor"):
        TransfermarktHttpClient(
            lease_provider=provider,
            lease_metadata=_metadata(),
            client_factory=factory,
        )

    assert provider.acquired == []
    assert factory.calls == []


@pytest.mark.unit
def test_small_grants_keep_a_proportional_soft_stop(monkeypatch):
    monkeypatch.setenv(
        PROVIDER_GRANT_ENV_VAR, str(PROVIDER_GRANT_FLOOR_BYTES),
    )
    client = TransfermarktHttpClient(
        lease_provider=_FakeLeaseProvider([]),
        lease_metadata=_metadata(),
        client_factory=_TlsFactory([]),
    )

    snapshot = client._traffic_ledger.snapshot()
    assert snapshot["hard_provider_byte_budget"] == PROVIDER_GRANT_FLOOR_BYTES
    assert snapshot["soft_provider_byte_stop"] == PROVIDER_GRANT_FLOOR_BYTES // 2


@pytest.mark.unit
def test_unreadable_provider_grant_fails_closed_before_any_io(monkeypatch):
    monkeypatch.setenv(PROVIDER_GRANT_ENV_VAR, "ten megabytes")
    provider = _FakeLeaseProvider([])
    factory = _TlsFactory([])

    with pytest.raises(TrafficMeterError, match="unreadable"):
        TransfermarktHttpClient(
            lease_provider=provider,
            lease_metadata=_metadata(),
            client_factory=factory,
        )

    assert provider.acquired == []
    assert factory.calls == []


@pytest.mark.unit
def test_a_metered_run_refuses_to_start_without_a_grant(monkeypatch):
    # Even without TM_REQUIRE_METERED_PROXY: a lease-backed client that found
    # no grant would silently inherit the full per-scope default ledger.
    monkeypatch.delenv(PROVIDER_GRANT_ENV_VAR, raising=False)
    monkeypatch.delenv("TM_REQUIRE_METERED_PROXY", raising=False)
    provider = _FakeLeaseProvider([])
    factory = _TlsFactory([])

    with pytest.raises(ProxyRequiredError, match=PROVIDER_GRANT_ENV_VAR):
        TransfermarktHttpClient(
            lease_provider=provider,
            lease_metadata=_metadata(),
            client_factory=factory,
        )

    assert provider.acquired == []
    assert factory.calls == []


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
def test_request_permit_is_polled_and_consumed_once_before_source_io():
    clock = [100.0]
    permit_id = "a" * 64
    permit_token = "p" * 32
    control = _ControlClient([
        _ControlResponse(429, {
            "code": "request_permit_pending",
            "granted": False,
            "retry_after_seconds": 5.0,
        }),
        _ControlResponse(200, {
            "schema_version": 1,
            "permit_id": permit_id,
            "permit_token": permit_token,
            "traffic_class": "transfermarkt",
            "granted": True,
            "expires_at_epoch": 135.0,
        }),
        _ControlResponse(200, {
            "schema_version": 1,
            "permit_id": permit_id,
            "traffic_class": "transfermarkt",
            "consumed": True,
            "consumed_at_epoch": 105.0,
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
        metadata=_metadata(),
        request_id="request-1",
    )

    assert granted == permit_id
    assert [call[1].rsplit("/", 1)[-1] for call in control.calls] == [
        "request-permits",
        "request-permits",
        "consume",
    ]
    assert control.calls[-1][2]["json"] == {
        "dag_id": "dag_ingest_transfermarkt",
        "run_id": "run-1",
        "request_id": "request-1",
        "permit_id": permit_id,
        "permit_token": permit_token,
    }


@pytest.mark.unit
def test_metered_http_attempt_requires_a_consumed_source_permit():
    provider = _FakeLeaseProvider([
        LeaseTrafficSnapshot(up_bytes=10, down_bytes=90),
    ])
    client = TransfermarktHttpClient(
        lease_provider=provider,
        traffic_ledger=SharedTrafficLedger(),
        lease_metadata=_metadata(),
        client_factory=_TlsFactory([_Response(b"ok")]),
    )

    outcome = client.fetch(
        "https://www.transfermarkt.com/page",
        as_json=False,
    )

    assert outcome.status is FetchStatus.OK
    assert len(provider.permits) == 1
    assert len(provider.permits[0][1]) == 64


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
def test_blocked_lease_rotates_once_and_never_direct_falls_back(monkeypatch):
    monkeypatch.setenv(PROVIDER_GRANT_ENV_VAR, str(8 * 1024 * 1024))
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
        raw_capture_id="a" * 64,
    ).as_checkpoint()
    legacy['version'] = 3
    legacy.pop('raw_attempt_envelope_id')
    legacy.pop('raw_attempt_envelope_ids')

    restored = FetchOutcome.from_checkpoint(legacy)
    assert restored.status is FetchStatus.AUTHORITATIVE_EMPTY
    assert restored.is_success is True
    assert restored.raw_capture_id == "a" * 64
    assert restored.with_status(FetchStatus.OK).raw_capture_id == "a" * 64
    assert FetchOutcome(
        status=FetchStatus.NOT_APPLICABLE,
        value=None,
    ).is_success is True

    corrupt = dict(legacy)
    corrupt["value"] = ["unexpected"]
    with pytest.raises(ValueError, match="hash mismatch"):
        FetchOutcome.from_checkpoint(corrupt)


@pytest.mark.unit
def test_checkpoint_preserves_raw_capture_lineage():
    digest = "a" * 64
    first_attempt = "c" * 64
    final_attempt = "d" * 64
    saved = FetchOutcome(
        status=FetchStatus.OK,
        value={"items": []},
        raw_capture_id=digest,
        raw_body_hash="b" * 64,
        raw_uri=f"s3://warehouse/raw/transfermarkt/{digest}.body.gz",
        raw_fetched_at="2026-07-21T12:00:00+00:00",
        raw_attempt_envelope_id=final_attempt,
        raw_attempt_envelope_ids=(first_attempt, final_attempt),
        attempts=2,
    ).as_checkpoint()

    restored = FetchOutcome.from_checkpoint(saved)

    assert restored.raw_capture_id == digest
    assert restored.raw_body_hash == "b" * 64
    assert restored.raw_uri.endswith(f"/{digest}.body.gz")
    assert restored.raw_fetched_at == "2026-07-21T12:00:00+00:00"
    assert restored.raw_attempt_envelope_id == final_attempt
    assert restored.raw_attempt_envelope_ids == (first_attempt, final_attempt)
    assert restored.with_status(FetchStatus.OK).raw_attempt_envelope_ids == (
        first_attempt,
        final_attempt,
    )


@pytest.mark.unit
def test_response_is_committed_and_reloaded_before_json_parse(tmp_path):
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    body = b'{"items":[1],"source":"stored-bytes"}\n'
    factory = _TlsFactory([
        _Response(body, payload=AssertionError("resp.json must not run")),
    ])
    client = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        raw_store=raw_store,
        require_raw_store=True,
        lease_metadata=_metadata(),
        client_factory=factory,
    )

    outcome = client.fetch(
        "https://www.transfermarkt.com/ceapi/example",
        as_json=True,
        label="transfer_events",
        context={"scope_id": "GB1:2025"},
    )

    assert outcome.status is FetchStatus.OK
    assert outcome.value == {"items": [1], "source": "stored-bytes"}
    assert outcome.raw_capture_id
    assert outcome.raw_body_hash
    assert outcome.raw_uri.endswith(".body.gz")
    assert outcome.raw_attempt_envelope_id
    assert outcome.raw_attempt_envelope_ids == (
        outcome.raw_attempt_envelope_id,
    )
    replayed, record = raw_store.load_capture(outcome.raw_capture_id)
    assert replayed == body
    assert record.content_hash == outcome.raw_body_hash
    assert client.get_raw_capture_records()[0]["capture_id"] == (
        outcome.raw_capture_id
    )
    envelope = raw_store.load_attempt_envelope(
        outcome.raw_attempt_envelope_id
    )
    assert envelope.outcome_kind == "response"
    assert envelope.capture_id == outcome.raw_capture_id
    assert raw_store.replay_attempt(envelope.envelope_id) == body
    assert client.get_raw_attempt_records() == ({**envelope.__dict__},)


@pytest.mark.unit
def test_transport_and_response_attempts_are_enveloped_before_retry(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv(PROVIDER_GRANT_ENV_VAR, str(8 * 1024 * 1024))
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    secret = "proxy-password-must-not-leak"
    factory = _TlsFactory([
        ConnectionError(
            f"TLS handshake via http://lease:{secret}@proxy_filter:8900 failed"
        ),
        _Response(b"ok"),
    ])
    provider = _FakeLeaseProvider([
        LeaseTrafficSnapshot(up_bytes=2, down_bytes=3),
        LeaseTrafficSnapshot(up_bytes=4, down_bytes=8),
    ])
    client = TransfermarktHttpClient(
        lease_provider=provider,
        lease_metadata=_metadata(),
        raw_store=raw_store,
        require_raw_store=True,
        client_factory=factory,
        sleep_fn=lambda _: None,
    )

    outcome = client.fetch(
        "https://www.transfermarkt.com/page",
        as_json=False,
        max_attempts=2,
        label="competition_page",
        context={"scope_id": "GB1:2025"},
    )

    assert outcome.status is FetchStatus.OK
    assert outcome.attempts == 2
    assert len(outcome.raw_attempt_envelope_ids) == 2
    attempts = tuple(
        raw_store.load_attempt_envelope(envelope_id)
        for envelope_id in outcome.raw_attempt_envelope_ids
    )
    assert [item.outcome_kind for item in attempts] == [
        "transport_error",
        "response",
    ]
    assert attempts[0].capture_id is None
    assert attempts[0].error_kind == "tls"
    assert attempts[0].error_type == "ConnectionError"
    assert attempts[1].capture_id == outcome.raw_capture_id
    assert outcome.raw_attempt_envelope_id == attempts[-1].envelope_id
    evidence = b"".join(
        raw_store._read_bytes(raw_store.attempt_manifest_key(item.envelope_id))
        for item in attempts
    )
    assert secret.encode() not in evidence
    assert b"error_message" not in evidence
    assert len(client.get_raw_attempt_records()) == 2


@pytest.mark.unit
def test_backfill_raw_envelope_prefers_frozen_child_cycle_identity(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("TM_CHILD_CYCLE_ID", "tm-child-frozen-scope")
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    client = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        lease_metadata={"run_id": "parent-batch-run", "scope": "GB1__2020"},
        raw_store=raw_store,
        require_raw_store=True,
        client_factory=_TlsFactory([_Response(b"ok")]),
        sleep_fn=lambda _: None,
    )

    outcome = client.fetch(
        "https://www.transfermarkt.com/page",
        as_json=False,
        max_attempts=1,
        label="listing",
        context={
            "scope_id": "GB1__2020",
            "cycle_id": "parent-batch-run",
        },
    )

    envelope = raw_store.verify_attempt_envelope(
        outcome.raw_attempt_envelope_id
    )
    assert envelope.cycle_id == "tm-child-frozen-scope"
    assert envelope.scope_id == "GB1__2020"


@pytest.mark.unit
def test_raw_store_failure_never_burns_a_second_paid_attempt():
    class FailingRawStore:
        def store_attempt(self, **_kwargs):
            raise RawStoreError("object store unavailable")

    factory = _TlsFactory([_Response(b"first"), _Response(b"must-not-run")])
    client = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        raw_store=FailingRawStore(),
        require_raw_store=True,
        client_factory=factory,
        sleep_fn=lambda _: None,
    )

    with pytest.raises(RawStoreError, match="unavailable"):
        client.fetch(
            "https://www.transfermarkt.com/page",
            as_json=False,
            max_attempts=3,
        )

    assert len(factory.clients[0].calls) == 1
    assert client.get_traffic_stats()["requests"] == 1


@pytest.mark.unit
def test_response_envelope_failure_never_burns_a_second_paid_attempt(tmp_path):
    inner = RawResponseStore.from_uri((tmp_path / "raw").as_uri())

    class FailingEnvelopeStore:
        store_attempt = inner.store_attempt
        load_capture = inner.load_capture

        @staticmethod
        def store_response_envelope(_record):
            raise OSError("attempt manifest store unavailable")

    factory = _TlsFactory([_Response(b"first"), _Response(b"must-not-run")])
    client = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        raw_store=FailingEnvelopeStore(),
        require_raw_store=True,
        client_factory=factory,
        sleep_fn=lambda _: None,
    )

    with pytest.raises(RawStoreError, match="failed closed"):
        client.fetch(
            "https://www.transfermarkt.com/page",
            as_json=False,
            max_attempts=3,
        )

    assert len(factory.clients[0].calls) == 1


@pytest.mark.unit
def test_transport_envelope_failure_never_burns_a_retry():
    class FailingEnvelopeStore:
        @staticmethod
        def store_transport_error(**_kwargs):
            raise OSError("attempt manifest store unavailable")

    factory = _TlsFactory([
        TimeoutError("secret proxy credential"),
        _Response(b"must-not-run"),
    ])
    client = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        raw_store=FailingEnvelopeStore(),
        require_raw_store=True,
        client_factory=factory,
        sleep_fn=lambda _: None,
    )

    with pytest.raises(RawStoreError, match="failed closed"):
        client.fetch(
            "https://www.transfermarkt.com/page",
            as_json=False,
            max_attempts=3,
        )

    assert len(factory.clients[0].calls) == 1


@pytest.mark.unit
def test_cache_hit_is_replayed_from_verified_raw_without_network(tmp_path):
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    cache = {}
    url = "https://www.transfermarkt.com/ceapi/cached"
    first = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        raw_store=raw_store,
        require_raw_store=True,
        cache=cache,
        client_factory=_TlsFactory([_Response(b'{"items":[]}' )]),
    )
    paid = first.fetch(
        url,
        as_json=True,
        cache_key=url,
        cache_ttl_seconds=60,
    )
    empty_factory = _TlsFactory([])
    second = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        raw_store=raw_store,
        require_raw_store=True,
        cache=cache,
        client_factory=empty_factory,
    )

    reused = second.fetch(
        url,
        as_json=True,
        cache_key=url,
        cache_ttl_seconds=60,
    )

    assert reused.cache_hit is True
    assert reused.raw_capture_id == paid.raw_capture_id
    assert reused.value == {"items": []}
    assert empty_factory.calls == []


@pytest.mark.unit
def test_cache_from_another_child_cycle_is_a_paid_miss(
    monkeypatch,
    tmp_path,
):
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    cache = {}
    url = "https://www.transfermarkt.com/ceapi/cycle-bound"
    monkeypatch.setenv("TM_CHILD_CYCLE_ID", "tm-child-A")
    first = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        raw_store=raw_store,
        require_raw_store=True,
        cache=cache,
        client_factory=_TlsFactory([_Response(b"cycle-A")]),
    )
    paid_a = first.fetch(
        url,
        as_json=False,
        cache_key=url,
        cache_ttl_seconds=3600,
    )
    envelope_a = raw_store.verify_attempt_envelope(
        paid_a.raw_attempt_envelope_id
    )

    monkeypatch.setenv("TM_CHILD_CYCLE_ID", "tm-child-B")
    second_factory = _TlsFactory([_Response(b"cycle-B")])
    second = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        raw_store=raw_store,
        require_raw_store=True,
        cache=cache,
        client_factory=second_factory,
    )
    paid_b = second.fetch(
        url,
        as_json=False,
        cache_key=url,
        cache_ttl_seconds=3600,
    )
    envelope_b = raw_store.verify_attempt_envelope(
        paid_b.raw_attempt_envelope_id
    )

    assert envelope_a.cycle_id == "tm-child-A"
    assert paid_b.cache_hit is False
    assert paid_b.value == "cycle-B"
    assert envelope_b.cycle_id == "tm-child-B"
    assert paid_b.raw_capture_id != paid_a.raw_capture_id
    assert len(second_factory.clients[0].calls) == 1


@pytest.mark.unit
def test_v3_cache_migrates_to_response_envelope_without_network(tmp_path):
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    cache = {}
    url = "https://www.transfermarkt.com/ceapi/cached"
    first = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        raw_store=raw_store,
        require_raw_store=True,
        cache=cache,
        client_factory=_TlsFactory([_Response(b'{"items":[]}' )]),
    )
    paid = first.fetch(
        url,
        as_json=True,
        cache_key=url,
        cache_ttl_seconds=60,
    )
    cached = cache[url]["outcome"]
    cached["version"] = 3
    cached.pop("raw_attempt_envelope_id")
    cached.pop("raw_attempt_envelope_ids")
    empty_factory = _TlsFactory([])
    second = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        raw_store=raw_store,
        require_raw_store=True,
        cache=cache,
        client_factory=empty_factory,
    )

    reused = second.fetch(
        url,
        as_json=True,
        cache_key=url,
        cache_ttl_seconds=60,
    )

    assert reused.cache_hit is True
    assert reused.raw_capture_id == paid.raw_capture_id
    assert reused.raw_attempt_envelope_id
    envelope = raw_store.verify_attempt(reused.raw_attempt_envelope_id)
    assert envelope.capture_id == paid.raw_capture_id
    assert empty_factory.calls == []


@pytest.mark.unit
def test_airflow_try_number_raw_ordinal_is_accepted(monkeypatch, tmp_path):
    monkeypatch.setenv("AIRFLOW_CTX_TRY_NUMBER", "1")
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    client = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        raw_store=raw_store,
        require_raw_store=True,
        client_factory=_TlsFactory([_Response(b"ok")]),
    )

    outcome = client.fetch(
        "https://www.transfermarkt.com/page",
        as_json=False,
    )

    assert outcome.status is FetchStatus.OK
    envelope = raw_store.verify_attempt_envelope(
        outcome.raw_attempt_envelope_id
    )
    assert envelope.attempt == 1_000_001


@pytest.mark.unit
@pytest.mark.parametrize("version", [4, 999])
def test_missing_or_future_attempt_chain_is_never_reused(
    tmp_path,
    version,
):
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    cache = {}
    url = "https://www.transfermarkt.com/ceapi/cached"
    first = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        raw_store=raw_store,
        require_raw_store=True,
        cache=cache,
        client_factory=_TlsFactory([_Response(b'{"items":[]}')]),
    )
    first.fetch(url, as_json=True, cache_key=url, cache_ttl_seconds=60)
    cached = cache[url]["outcome"]
    cached["version"] = version
    cached.pop("raw_attempt_envelope_id")
    cached.pop("raw_attempt_envelope_ids")
    empty_factory = _TlsFactory([])
    second = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        raw_store=raw_store,
        require_raw_store=True,
        cache=cache,
        client_factory=empty_factory,
    )

    outcome = second.fetch(
        url,
        as_json=True,
        cache_key=url,
        cache_ttl_seconds=60,
        max_attempts=1,
    )

    assert empty_factory.calls
    assert outcome.cache_hit is False
    assert outcome.status is FetchStatus.RETRY_EXHAUSTED


@pytest.mark.unit
def test_transport_fetch_error_never_contains_bearer_or_control_token(
    monkeypatch,
    tmp_path,
):
    secret = "top-secret-control-token-value-123456"
    monkeypatch.setenv("TM_PROXY_CONTROL_TOKEN", secret)
    raw_store = RawResponseStore.from_uri((tmp_path / "raw").as_uri())
    client = TransfermarktHttpClient(
        proxy="http://paid-proxy.invalid:8000",
        raw_store=raw_store,
        require_raw_store=True,
        client_factory=_TlsFactory([
            TimeoutError(f"Authorization: Bearer {secret}"),
        ]),
        sleep_fn=lambda _: None,
    )

    outcome = client.fetch(
        "https://www.transfermarkt.com/page",
        as_json=False,
        max_attempts=1,
    )

    assert secret not in str(outcome.error)
    assert "Bearer" not in str(outcome.error)
    assert outcome.error == "transport:timeout:TimeoutError"


@pytest.mark.unit
def test_required_raw_store_is_checked_before_transport(monkeypatch):
    monkeypatch.delenv("TRANSFERMARKT_RAW_STORE_URI", raising=False)
    with pytest.raises(RawStoreError, match="TRANSFERMARKT_RAW_STORE_URI"):
        TransfermarktHttpClient(
            proxy="http://paid-proxy.invalid:8000",
            require_raw_store=True,
            client_factory=_TlsFactory([_Response(b"must-not-run")]),
        )


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
