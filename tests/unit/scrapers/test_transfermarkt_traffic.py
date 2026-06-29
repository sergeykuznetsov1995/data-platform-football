"""Transfermarkt residential-proxy byte counter (issue #789).

The scraper passively counts response-body bytes per host so the daily proxy
report can attribute residential spend to Transfermarkt. The counter must:
count every status (a 403 CF page is billed too), aggregate by host, and never
raise on a malformed response.
"""

import pytest

from scrapers.transfermarkt import TransfermarktScraper


class _FakeResp:
    def __init__(self, body: bytes, status: int = 200):
        self.content = body
        self.status_code = status


@pytest.fixture
def scraper():
    return TransfermarktScraper(leagues=['ENG-Premier League'], seasons=[2025])


@pytest.mark.unit
def test_record_proxy_bytes_accumulates_by_host(scraper):
    scraper._record_proxy_bytes('https://www.transfermarkt.us/a', _FakeResp(b'a' * 1000))
    scraper._record_proxy_bytes('https://www.transfermarkt.us/b', _FakeResp(b'b' * 2000))

    stats = scraper.get_traffic_stats()

    assert stats['proxy_response_bytes'] == 3000
    assert stats['proxy_response_mb'] == round(3000 / 1024 / 1024, 4)
    assert stats['top_traffic_urls'][0] == {
        'url': 'www.transfermarkt.us',
        'bytes': 3000,
        'mb': round(3000 / 1024 / 1024, 4),
    }


@pytest.mark.unit
def test_counts_error_pages_too(scraper):
    # A 403 CF block page still costs residential bytes — must be counted.
    scraper._record_proxy_bytes(
        'https://www.transfermarkt.us/z', _FakeResp(b'x' * 500, status=403)
    )

    assert scraper.get_traffic_stats()['proxy_response_bytes'] == 500


@pytest.mark.unit
def test_counter_never_raises_on_bad_response(scraper):
    class _Bad:
        @property
        def content(self):
            raise RuntimeError('stream gone')

    scraper._record_proxy_bytes('https://x', _Bad())  # must not raise

    assert scraper.get_traffic_stats()['proxy_response_bytes'] == 0


@pytest.mark.unit
def test_zero_traffic_shape(scraper):
    assert scraper.get_traffic_stats() == {
        'proxy_response_bytes': 0,
        'proxy_response_mb': 0.0,
        'requests': 0,
        'top_traffic_urls': [],
    }
