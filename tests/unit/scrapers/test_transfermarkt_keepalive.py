"""#1388: one keep-alive TLS session per proxy lease, not per request."""

from __future__ import annotations

import http.server
import select
import shutil
import socket
import ssl
import subprocess
import threading
from dataclasses import replace

import pytest

from scrapers.transfermarkt.client import TransfermarktHttpClient
from scrapers.transfermarkt.models import (
    FetchStatus,
    LeaseTrafficSnapshot,
    ProxyLease,
    SharedTrafficLedger,
)


class _Response:
    def __init__(self, body: bytes):
        self.content = body
        self.status_code = 200
        self.headers = {"Content-Length": str(len(body))}

    @property
    def text(self):
        return self.content.decode("utf-8")


class _TlsClient:
    def __init__(self):
        self.closed = False

    def get(self, url, **kwargs):
        return _Response(b"page")

    def close(self):
        self.closed = True


class _TlsFactory:
    def __init__(self):
        self.calls = []
        self.clients = []

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        self.clients.append(_TlsClient())
        return self.clients[-1]


class _LeaseProvider:
    """Leases that expire ``ttl`` seconds after the injected clock."""

    def __init__(self, clock, ttl, permit_wait=None):
        self.clock = clock
        self.ttl = ttl
        self.permit_wait = permit_wait
        self.acquired = []
        self.closed = []

    def acquire(self, *, max_bytes, ttl_seconds, metadata):
        lease = ProxyLease(
            lease_id=f"lease-{len(self.acquired) + 1}",
            token=f"token-{len(self.acquired) + 1}",
            proxy_url="http://proxy_filter:8900",
            max_bytes=max_bytes,
            expires_at=self.clock() + self.ttl,
        )
        self.acquired.append(lease)
        return lease

    def stats(self, lease):
        return LeaseTrafficSnapshot(up_bytes=10, down_bytes=90)

    def close(self, lease):
        self.closed.append(lease.lease_id)
        return replace(LeaseTrafficSnapshot(up_bytes=10, down_bytes=90), closed=True)

    def acquire_request_permit(self, *, metadata, request_id):
        if self.permit_wait is not None:
            self.permit_wait()
        return "permit"

    @staticmethod
    def authenticated_proxy_url(lease):
        return f"http://lease:{lease.token}@proxy_filter:8900"


def _metered_client(clock, ttl, permit_wait=None):
    provider = _LeaseProvider(clock, ttl, permit_wait)
    factory = _TlsFactory()
    client = TransfermarktHttpClient(
        lease_provider=provider,
        traffic_ledger=SharedTrafficLedger(),
        lease_metadata={
            "dag_id": "dag_ingest_transfermarkt",
            "run_id": "run-1",
            "task_id": "capture_scope",
            "scope": "GB1/2025",
        },
        client_factory=factory,
        time_fn=clock,
    )
    return client, provider, factory


@pytest.mark.unit
def test_one_session_id_per_lease_counts_requests_per_session():
    now = [1_000.0]
    client, provider, factory = _metered_client(lambda: now[0], ttl=3_600)

    for path in ("a", "b", "c"):
        assert client.fetch(
            f"https://www.transfermarkt.us/{path}", as_json=False, label="mv",
        ).status is FetchStatus.OK

    assert len(provider.acquired) == 1
    assert len(factory.calls) == 1
    assert factory.calls[0]["session_id"]
    assert client.get_traffic_stats()["requests_per_session"] == {
        "sessions": 1,
        "requests": 3,
        "multi_request_sessions": 1,
        "max_requests_per_session": 3,
    }
    client.close()
    assert factory.clients[0].closed is True
    assert provider.closed == ["lease-1"]


@pytest.mark.unit
def test_expiring_lease_is_rotated_before_the_request_with_a_new_session():
    now = [1_000.0]
    client, provider, factory = _metered_client(lambda: now[0], ttl=100)

    client.fetch("https://www.transfermarkt.us/a", as_json=False, label="mv")
    now[0] += 30  # 70 s left: keep the session
    client.fetch("https://www.transfermarkt.us/b", as_json=False, label="mv")
    now[0] += 20  # 50 s left (< 60): planned close, new lease and session
    outcome = client.fetch(
        "https://www.transfermarkt.us/c", as_json=False, label="mv",
    )

    assert outcome.status is FetchStatus.OK
    assert provider.closed == ["lease-1"]
    assert len(provider.acquired) == 2
    assert factory.clients[0].closed is True
    first, second = (call["session_id"] for call in factory.calls)
    assert first and second and first != second
    assert client.get_traffic_stats()["requests_per_session"] == {
        "sessions": 2,
        "requests": 3,
        "multi_request_sessions": 1,
        "max_requests_per_session": 2,
    }


@pytest.mark.unit
def test_lease_expiry_is_checked_after_the_permit_wait():
    now = [1_000.0]

    def permit_wait():
        now[0] += 65  # the longest bounded permit poll

    client, provider, factory = _metered_client(
        lambda: now[0], ttl=200, permit_wait=permit_wait,
    )

    # 200 s lease: after the first wait 135 s are left -> same session.
    client.fetch("https://www.transfermarkt.us/a", as_json=False, label="mv")
    # Before the second wait 135 s are left (would pass a pre-wait check),
    # after it only 70 s; after the third wait 5 s -> rotate before I/O.
    client.fetch("https://www.transfermarkt.us/b", as_json=False, label="mv")
    client.fetch("https://www.transfermarkt.us/c", as_json=False, label="mv")

    assert provider.closed == ["lease-1"]
    assert len(provider.acquired) == 2
    assert provider.acquired[1].expires_at - now[0] == 200
    assert client.get_traffic_stats()["requests_per_session"]["sessions"] == 2
    assert len({call["session_id"] for call in factory.calls}) == 2


# --- real sockets: local CONNECT proxy counting accepted connections -------


class _PageHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        body = b"x" * 100
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class _ConnectProxy:
    def __init__(self):
        self.accepts = 0
        self._sock = socket.socket()
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self.url = f"http://127.0.0.1:{self._sock.getsockname()[1]}"
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            self.accepts += 1
            threading.Thread(target=self._tunnel, args=(conn,), daemon=True).start()

    @staticmethod
    def _tunnel(conn):
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = conn.recv(4096)
            if not chunk:
                conn.close()
                return
            head += chunk
        host, port = head.split(b" ")[1].decode().rsplit(":", 1)
        upstream = socket.create_connection((host, int(port)))
        conn.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
        try:
            while True:
                ready, _, _ = select.select([conn, upstream], [], [], 5)
                for side in ready:
                    data = side.recv(65536)
                    if not data:
                        return
                    (upstream if side is conn else conn).sendall(data)
        except OSError:
            return
        finally:
            conn.close()
            upstream.close()

    def close(self):
        self._sock.close()


@pytest.fixture
def https_page(tmp_path):
    if shutil.which("openssl") is None:
        pytest.skip("openssl is required for the self-signed test certificate")
    pytest.importorskip("tls_requests")
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(key), "-out", str(cert), "-days", "1",
            "-subj", "/CN=127.0.0.1",
        ],
        check=True, capture_output=True,
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _PageHandler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(cert), str(key))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    proxy = _ConnectProxy()
    try:
        yield f"https://127.0.0.1:{server.server_address[1]}", proxy
    finally:
        proxy.close()
        server.shutdown()
        server.server_close()


def _count_connections(base_url, proxy, *, keep_session_id):
    import tls_requests

    def factory(**kwargs):
        if not keep_session_id:
            kwargs.pop("session_id", None)
        # Test-only: the local page has a self-signed certificate.
        return tls_requests.Client(verify=False, **kwargs)

    client = TransfermarktHttpClient(
        proxy=proxy.url, client_factory=factory, timeout_seconds=5,
    )
    try:
        for index in range(5):
            outcome = client.fetch(
                f"{base_url}/page/{index}", as_json=False, max_attempts=1,
            )
            assert outcome.status is FetchStatus.OK, outcome.error
    finally:
        client.close()
    return proxy.accepts


@pytest.mark.unit
def test_five_requests_share_one_proxy_connection(https_page):
    base_url, proxy = https_page

    assert _count_connections(base_url, proxy, keep_session_id=True) == 1


@pytest.mark.unit
def test_control_without_session_id_opens_a_connection_per_request(https_page):
    base_url, proxy = https_page

    assert _count_connections(base_url, proxy, keep_session_id=False) == 5
