"""
Tests for FlareSolverrWhoScoredReader.

Strategy: bypass the real ``sd.WhoScored.__init__`` (which would init a
selenium driver and create cache dirs against the host filesystem) via
``patch.object(sd.WhoScored, '__init__')``. Tests target the HTTP-transport
overrides — ``_download_and_save`` (var=None / var=<name> branches),
``<pre>`` unwrapping for ``.json`` filepaths, session lifecycle, rotation
on CF challenge / timeout, recovery exhaustion. The actual soccerdata
``read_schedule`` integration is exercised by the integration smoke against
the Docker stack, not these unit tests.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_fs_client():
    """Patch FlareSolverrClient class with a MagicMock factory."""
    with patch('scrapers.whoscored.flaresolverr_reader.FlareSolverrClient') as cls:
        instance = MagicMock()
        cls.return_value = instance
        yield instance


@pytest.fixture
def reader(mock_fs_client):
    """Construct a FlareSolverrWhoScoredReader with sd.WhoScored.__init__ stubbed."""
    import soccerdata as sd
    from scrapers.whoscored.flaresolverr_reader import FlareSolverrWhoScoredReader

    with patch.object(sd.WhoScored, '__init__', return_value=None):
        r = FlareSolverrWhoScoredReader(
            flaresolverr_url='http://flaresolverr:8191',
            proxy=None,
            max_timeout_ms=90_000,
            leagues=['ENG-Premier League'],
            seasons=[2025],
        )
    # sd.WhoScored.__init__ was stubbed — set attributes the base class would have:
    r.no_store = True
    r.no_cache = True
    yield r


class TestExtractJsVar:
    """Standalone tests for the JS-var extractor used to replace
    ``driver.execute_script("return <name>")`` under FlareSolverr."""

    def test_var_keyword_prefix(self):
        from scrapers.whoscored.flaresolverr_reader import _extract_js_var
        html = '<script>var wsCalendar = {"mask": {"2025": ["10", "11"]}};</script>'
        assert _extract_js_var(html, 'wsCalendar') == {
            'mask': {'2025': ['10', '11']}
        }

    def test_bare_assignment(self):
        from scrapers.whoscored.flaresolverr_reader import _extract_js_var
        html = '<script>allRegions = [{"id": 252, "name": "England"}];</script>'
        # bare assignment is the form used by WhoScored for some globals
        # — but our extractor scans for the {...} block; an array literal
        # is also a valid JSON top-level value, so we test the dict case
        # which is what WhoScored actually emits.
        html = '<script>allRegions = {"x": 1};</script>'
        assert _extract_js_var(html, 'allRegions') == {'x': 1}

    def test_string_awareness(self):
        """Braces inside string values must not unbalance the scanner."""
        from scrapers.whoscored.flaresolverr_reader import _extract_js_var
        # JSON-safe: braces inside a string literal
        html = 'var data = {"label": "a{b}c", "n": 1};'
        assert _extract_js_var(html, 'data') == {'label': 'a{b}c', 'n': 1}

    def test_missing_var_raises(self):
        from scrapers.whoscored.flaresolverr_reader import _extract_js_var
        with pytest.raises(ValueError, match="not found"):
            _extract_js_var('<html>no JS here</html>', 'wsCalendar')

    def test_ws_calendar_with_new_date_calls(self):
        """wsCalendar embeds ``(new Date(...)).toString()`` JS expressions.
        soccerdata reads only the ``mask`` field, so stripping date calls
        to ``null`` keeps the blob parseable without losing useful data."""
        from scrapers.whoscored.flaresolverr_reader import _extract_js_var
        # Trimmed real WhoScored wsCalendar format.
        html = (
            "var wsCalendar = {\n"
            "  min: (new Date(2025, 7, 15)).toString(),\n"
            "  max: (new Date(2026, 4, 24)).toString(),\n"
            "  fixtureDate: (new Date(2026, 4, 24, 0, 0, 0)).toString(),\n"
            "  mask: {2025:{7:{15:1,16:1}}, 2026:{0:{1:1}}}\n"
            "};"
        )
        result = _extract_js_var(html, 'wsCalendar')
        assert result['mask'] == {
            '2025': {'7': {'15': 1, '16': 1}},
            '2026': {'0': {'1': 1}},
        }
        # date-call fields stripped to null
        assert result['min'] is None and result['max'] is None

    def test_escaped_single_quote_inside_string(self):
        """WhoScored embeds 'Papa John\\'s Trophy'-style escaped quotes."""
        from scrapers.whoscored.flaresolverr_reader import _extract_js_var
        html = "var x = [{name:'Papa John\\'s Trophy', id:23}];"
        assert _extract_js_var(html, 'x') == [
            {'name': "Papa John's Trophy", 'id': 23}
        ]

    def test_array_top_level(self):
        """allRegions is an array literal, not an object."""
        from scrapers.whoscored.flaresolverr_reader import _extract_js_var
        html = '<script>var allRegions = [{"id": 252, "name": "England"}];</script>'
        assert _extract_js_var(html, 'allRegions') == [
            {'id': 252, 'name': 'England'}
        ]

    def test_js_literal_unquoted_keys_and_single_quotes(self):
        """Real WhoScored format: unquoted keys + single-quoted strings."""
        from scrapers.whoscored.flaresolverr_reader import _extract_js_var
        # Real-shape WhoScored snippet (allRegions homepage embed).
        html = (
            "<script>var allRegions = [{type:1, id:248, flg:'flg-caf', "
            "name: 'Africa', tournaments: [{id:290, url:'/x', "
            "name:'Champions League'}]}];</script>"
        )
        result = _extract_js_var(html, 'allRegions')
        assert result == [
            {
                'type': 1, 'id': 248, 'flg': 'flg-caf', 'name': 'Africa',
                'tournaments': [
                    {'id': 290, 'url': '/x', 'name': 'Champions League'},
                ],
            },
        ]

    def test_malformed_json_raises(self):
        from scrapers.whoscored.flaresolverr_reader import _extract_js_var
        # Trailing comma + colon-after-comma → unparseable even with our
        # JS-literal normaliser.
        html = 'var bad = {"trailing": ,};'
        with pytest.raises(ValueError):
            _extract_js_var(html, 'bad')


class TestExtractPreBody:
    """The ``<pre>`` wrapper stripper used for JSON-endpoint caching."""

    def test_strips_pre_wrapper(self):
        from scrapers.whoscored.flaresolverr_reader import _extract_pre_body
        wrapped = '<html><body><pre>{"a": 1}</pre></body></html>'
        assert _extract_pre_body(wrapped) == '{"a": 1}'

    def test_html_unescape_inside_pre(self):
        from scrapers.whoscored.flaresolverr_reader import _extract_pre_body
        wrapped = '<html><body><pre>{&quot;a&quot;: 1}</pre></body></html>'
        assert _extract_pre_body(wrapped) == '{"a": 1}'

    def test_strips_body_wrapper_when_no_pre(self):
        """WhoScored /tournaments/.../data/?d=... ships JSON in plain <body>."""
        from scrapers.whoscored.flaresolverr_reader import _extract_pre_body
        wrapped = '<html><head></head><body>{"createdAt":"X","tournaments":[]}</body></html>'
        assert _extract_pre_body(wrapped) == (
            '{"createdAt":"X","tournaments":[]}'
        )

    def test_passthrough_when_no_wrapper(self):
        from scrapers.whoscored.flaresolverr_reader import _extract_pre_body
        plain = '{"a": 1}'
        assert _extract_pre_body(plain) == plain


class TestFlareSolverrWhoScoredReader:
    def test_init_creates_session(self, mock_fs_client, reader):
        """__init__ must allocate a FlareSolverr session before any HTTP traffic."""
        assert mock_fs_client.create_session.call_count == 1
        call = mock_fs_client.create_session.call_args
        sid = call.args[0] if call.args else call.kwargs['session_id']
        assert sid.startswith('whoscored-')
        assert reader._session_id == sid

    def test_init_webdriver_is_noop(self, reader):
        """_init_webdriver must NOT spawn a browser."""
        assert reader._init_webdriver() is None

    def test_download_and_save_var_none_html(self, mock_fs_client, reader, tmp_path):
        """var=None + non-.json filepath → return raw HTML bytes."""
        mock_fs_client.get.return_value = {
            'html': '<html><body><p>ok</p></body></html>',
            'status': 200,
        }
        fp = tmp_path / 'page.html'
        result = reader._download_and_save('https://whoscored.com/', filepath=fp)

        assert isinstance(result, io.BytesIO)
        assert b'<p>ok</p>' in result.getvalue()
        assert not fp.exists()  # no_store=True
        assert reader._request_count == 1
        args, kwargs = mock_fs_client.get.call_args
        assert args[0] == 'https://whoscored.com/'
        assert args[1] == reader._session_id
        assert kwargs.get('max_timeout_ms') == 90_000

    def test_download_and_save_var_none_json_strips_pre(self, mock_fs_client, reader, tmp_path):
        """var=None + .json filepath → strip <pre> wrapper so cached file is valid JSON."""
        mock_fs_client.get.return_value = {
            'html': '<html><body><pre>{"id": 42}</pre></body></html>',
            'status': 200,
        }
        fp = tmp_path / 'fixtures.json'
        result = reader._download_and_save('https://whoscored.com/x/data', filepath=fp)
        assert result.getvalue() == b'{"id": 42}'

    def test_download_and_save_var_extract(self, mock_fs_client, reader):
        """var=<name> → return json.dumps of extracted JS value."""
        mock_fs_client.get.return_value = {
            'html': '<script>var wsCalendar = {"mask": {"2025": ["10"]}};</script>',
            'status': 200,
        }
        result = reader._download_and_save(
            'https://whoscored.com/Regions/.../Stages/...',
            var='wsCalendar',
        )
        # json.dumps round-trips → parse back to compare structurally
        import json
        assert json.loads(result.getvalue()) == {'mask': {'2025': ['10']}}

    def test_download_and_save_var_missing_returns_null(self, mock_fs_client, reader):
        """Missing JS variable → json.dumps(None) (matches soccerdata Selenium contract)."""
        mock_fs_client.get.return_value = {
            'html': '<html>no JS var here</html>',
            'status': 200,
        }
        result = reader._download_and_save(
            'https://whoscored.com/p',
            var='wsCalendar',
        )
        assert result.getvalue() == b'null'

    def test_download_and_save_var_iterable_raises(self, reader, mock_fs_client):
        """Multi-var extraction not supported (matches soccerdata)."""
        mock_fs_client.get.return_value = {'html': '<html>x</html>', 'status': 200}
        with pytest.raises(NotImplementedError):
            reader._download_and_save('https://whoscored.com/', var=['a', 'b'])

    def test_download_and_save_persists_when_store_enabled(self, mock_fs_client, reader, tmp_path):
        mock_fs_client.get.return_value = {'html': '<html>X</html>', 'status': 200}
        reader.no_store = False
        fp = tmp_path / 'sub' / 'page.html'
        reader._download_and_save('https://whoscored.com/', filepath=fp)
        assert fp.exists()
        assert fp.read_text() == '<html>X</html>'

    def test_download_and_save_rejects_challenge_html(self, mock_fs_client, reader):
        from scrapers.base.flaresolverr_client import FlareSolverrCFChallengeFailed
        mock_fs_client.get.return_value = {
            'html': '<html>Just a moment <script>_cf_chl_opt = {};</script></html>',
            'status': 403,
        }
        with pytest.raises(FlareSolverrCFChallengeFailed):
            reader._download_and_save('https://whoscored.com/')

    def test_download_and_save_rejects_empty_body(self, mock_fs_client, reader):
        mock_fs_client.get.return_value = {'html': '', 'status': 200}
        with pytest.raises(ConnectionError):
            reader._download_and_save('https://whoscored.com/')

    @pytest.mark.parametrize(
        'html',
        [
            '<html><body>chrome-error://dino/</body></html>',
            '<html id="neterror"><body>x</body></html>',
            '<html><body>ERR_NO_SUPPORTED_PROXIES</body></html>',
        ],
    )
    def test_download_and_save_rejects_chromium_error_page(self, mock_fs_client, reader, html):
        """A Chromium net-error page (HTTP 200) must raise, not be returned (#655)."""
        from scrapers.base.flaresolverr_client import FlareSolverrErrorPage

        mock_fs_client.get.return_value = {'html': html, 'status': 200}
        with pytest.raises(FlareSolverrErrorPage):
            reader._download_and_save('https://whoscored.com/')

    def test_download_and_save_does_not_cache_error_page(self, mock_fs_client, reader, tmp_path):
        """Regression #655: error page must NOT poison the disk cache (var=None path)."""
        from scrapers.base.flaresolverr_client import FlareSolverrErrorPage

        reader.no_store = False
        fp = tmp_path / 'page.html'
        mock_fs_client.get.return_value = {
            'html': '<html><body>chrome-error://dino/ ERR_NO_SUPPORTED_PROXIES</body></html>',
            'status': 200,
        }
        with pytest.raises(FlareSolverrErrorPage):
            reader._download_and_save('https://whoscored.com/', filepath=fp)
        assert not fp.exists()

    def test_validate_page_is_defensive(self, reader):
        with pytest.raises(RuntimeError):
            reader._validate_page('https://whoscored.com/')

    def test_close_destroys_session_once(self, mock_fs_client, reader):
        reader.close()
        reader.close()  # idempotent
        assert mock_fs_client.destroy_session.call_count == 1

    def test_close_swallows_destroy_error(self, mock_fs_client, reader):
        mock_fs_client.destroy_session.side_effect = Exception('boom')
        reader.close()  # must not raise
        assert reader._session_closed is True

    def test_cf_challenge_triggers_rotation_and_retry(self, mock_fs_client, reader, monkeypatch):
        from scrapers.base.flaresolverr_client import FlareSolverrCFChallengeFailed
        monkeypatch.setattr('time.sleep', lambda *_: None)
        original_sid = reader._session_id
        mock_fs_client.get.side_effect = [
            FlareSolverrCFChallengeFailed('Error solving the challenge. Timeout after 90.0s.'),
            {'html': '<html>recovered</html>', 'status': 200},
        ]
        result = reader._download_and_save('https://whoscored.com/x')
        assert b'recovered' in result.getvalue()
        assert mock_fs_client.destroy_session.call_count == 1
        assert mock_fs_client.create_session.call_count == 2
        assert reader._session_id != original_sid
        assert reader._request_count == 1  # only the recovered call counts

    def test_timeout_triggers_rotation_and_retry(self, mock_fs_client, reader, monkeypatch):
        from scrapers.base.flaresolverr_client import FlareSolverrTimeout
        monkeypatch.setattr('time.sleep', lambda *_: None)
        original_sid = reader._session_id
        mock_fs_client.get.side_effect = [
            FlareSolverrTimeout('FlareSolverr unreachable'),
            {'html': '<html>ok</html>', 'status': 200},
        ]
        reader._download_and_save('https://whoscored.com/x')
        assert mock_fs_client.destroy_session.call_count == 1
        assert mock_fs_client.create_session.call_count == 2
        assert reader._session_id != original_sid

    def test_cf_challenge_propagates_after_max_recoveries(self, mock_fs_client, reader, monkeypatch):
        from scrapers.base.flaresolverr_client import FlareSolverrCFChallengeFailed
        monkeypatch.setattr('time.sleep', lambda *_: None)
        mock_fs_client.get.side_effect = [
            FlareSolverrCFChallengeFailed('Cloudflare')
            for _ in range(reader._max_recoveries + 1)
        ]
        with pytest.raises(FlareSolverrCFChallengeFailed):
            reader._download_and_save('https://whoscored.com/x')

    def test_session_rotates_every_n_requests(self, mock_fs_client, reader):
        mock_fs_client.get.return_value = {'html': '<html>X</html>', 'status': 200}
        reader._session_recreate_every = 2
        original_sid = reader._session_id

        reader._download_and_save('https://a/')  # request 1, no rotation
        sid_after_1 = reader._session_id
        reader._download_and_save('https://b/')  # request 2, rotation next call
        sid_after_2 = reader._session_id
        reader._download_and_save('https://c/')  # next call → recreate session

        assert sid_after_1 == original_sid
        assert sid_after_2 == original_sid
        assert reader._session_id != original_sid
        assert mock_fs_client.create_session.call_count == 2
        assert mock_fs_client.destroy_session.call_count == 1
