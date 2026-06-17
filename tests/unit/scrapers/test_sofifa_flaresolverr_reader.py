"""
Tests for FlareSolverrSoFIFAReader.

Strategy: bypass the real ``sd.SoFIFA.__init__`` (which would fire read_versions
against sofifa.com) via ``patch.object(sd.SoFIFA, '__init__')``. Tests target
the HTTP-transport overrides — _download_and_save, session lifecycle, session
rotation. The full integration of soccerdata parsing is covered by the
end-to-end smoke run, not unit tests.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_fs_client():
    """Patch FlareSolverrClient class with a MagicMock factory."""
    with patch('scrapers.sofifa.flaresolverr_reader.FlareSolverrClient') as cls:
        instance = MagicMock()
        cls.return_value = instance
        yield instance


@pytest.fixture
def reader(mock_fs_client):
    """Construct a FlareSolverrSoFIFAReader with sd.SoFIFA.__init__ stubbed."""
    import soccerdata as sd
    from scrapers.sofifa.flaresolverr_reader import FlareSolverrSoFIFAReader

    with patch.object(sd.SoFIFA, '__init__', return_value=None):
        r = FlareSolverrSoFIFAReader(
            flaresolverr_url='http://flaresolverr:8191',
            proxy=None,
            max_timeout_ms=90_000,
        )
    # sd.SoFIFA.__init__ was stubbed — set attributes that the base class would have:
    r.no_store = True
    r.no_cache = True
    yield r


class TestFlareSolverrSoFIFAReader:
    def test_init_creates_session(self, mock_fs_client, reader):
        """__init__ must allocate a FlareSolverr session before any HTTP traffic."""
        assert mock_fs_client.create_session.call_count == 1
        call = mock_fs_client.create_session.call_args
        # session_id is auto-generated; should start with our prefix
        sid = call.args[0] if call.args else call.kwargs['session_id']
        assert sid.startswith('sofifa-')
        assert reader._session_id == sid

    def test_init_webdriver_is_noop(self, reader):
        """_init_webdriver must NOT spawn a browser — that's the whole point."""
        assert reader._init_webdriver() is None

    def test_download_and_save_routes_through_flaresolverr(self, mock_fs_client, reader, tmp_path):
        from scrapers.sofifa.flaresolverr_reader import FlareSolverrSoFIFAReader  # noqa
        mock_fs_client.get.return_value = {
            'html': '<html><body><p>ok</p></body></html>',
            'status': 200,
            'cookies': [],
            'userAgent': 'X',
        }
        fp = tmp_path / 'index.html'

        result = reader._download_and_save('https://sofifa.com/', filepath=fp)

        assert isinstance(result, io.BytesIO)
        assert b'<p>ok</p>' in result.getvalue()
        # cache write: no_store=True in fixture → file NOT written
        assert not fp.exists()
        # request counter advanced
        assert reader._request_count == 1
        # fs_client.get called with right session; URL gets hl=en-US injected
        # to override sofifa.com geo-locale detection (otherwise NL/DE/etc)
        assert mock_fs_client.get.call_count == 1
        args, kwargs = mock_fs_client.get.call_args
        assert args[0] == 'https://sofifa.com/?hl=en-US'
        assert args[1] == reader._session_id
        assert kwargs.get('max_timeout_ms') == 90_000

    def test_download_and_save_persists_when_store_enabled(self, mock_fs_client, reader, tmp_path):
        mock_fs_client.get.return_value = {'html': '<html>X</html>', 'status': 200}
        reader.no_store = False
        fp = tmp_path / 'sub' / 'index.html'

        reader._download_and_save('https://sofifa.com/', filepath=fp)

        assert fp.exists()
        assert fp.read_text() == '<html>X</html>'

    def test_download_and_save_rejects_challenge_html(self, mock_fs_client, reader):
        from scrapers.base.flaresolverr_client import FlareSolverrCFChallengeFailed

        mock_fs_client.get.return_value = {
            'html': '<html>Just a moment <script>_cf_chl_opt = {};</script></html>',
            'status': 403,
        }
        with pytest.raises(FlareSolverrCFChallengeFailed):
            reader._download_and_save('https://sofifa.com/')

    def test_download_and_save_rejects_empty_body(self, mock_fs_client, reader):
        mock_fs_client.get.return_value = {'html': '', 'status': 200}
        with pytest.raises(ConnectionError):
            reader._download_and_save('https://sofifa.com/')

    def test_var_param_raises(self, reader):
        with pytest.raises(NotImplementedError):
            reader._download_and_save('https://sofifa.com/', var='something')

    def test_validate_page_is_defensive(self, reader):
        with pytest.raises(RuntimeError):
            reader._validate_page('https://sofifa.com/')

    def test_force_english_helper(self):
        from scrapers.sofifa.flaresolverr_reader import _force_english
        # appends ?hl=en-US to bare URL
        assert _force_english('https://sofifa.com/player/1/') == 'https://sofifa.com/player/1/?hl=en-US'
        # appends &hl=en-US when ? is already present
        assert _force_english('https://sofifa.com/teams?lg=13&r=260033') == 'https://sofifa.com/teams?lg=13&r=260033&hl=en-US'
        # idempotent: does not duplicate if hl= already in URL
        assert _force_english('https://sofifa.com/p/1?hl=en-US') == 'https://sofifa.com/p/1?hl=en-US'
        assert _force_english('https://sofifa.com/p/1?hl=fr-FR') == 'https://sofifa.com/p/1?hl=fr-FR'

    def test_close_destroys_session_once(self, mock_fs_client, reader):
        reader.close()
        reader.close()  # idempotent
        assert mock_fs_client.destroy_session.call_count == 1

    def test_close_swallows_destroy_error(self, mock_fs_client, reader):
        mock_fs_client.destroy_session.side_effect = Exception('boom')
        # should not raise
        reader.close()
        assert reader._session_closed is True

    def test_tab_crash_triggers_session_rotation_and_retry(self, mock_fs_client, reader, monkeypatch):
        """FlareSolverrTabCrashed → destroy + recreate session, then retry once."""
        from scrapers.base.flaresolverr_client import FlareSolverrTabCrashed

        monkeypatch.setattr('time.sleep', lambda *_: None)
        original_sid = reader._session_id
        # First call raises tab crash, second call (after rotation) succeeds.
        mock_fs_client.get.side_effect = [
            FlareSolverrTabCrashed('Error: tab crashed (chrome=142)'),
            {'html': '<html>recovered</html>', 'status': 200},
        ]

        result = reader._download_and_save('https://sofifa.com/team/9/')

        assert b'recovered' in result.getvalue()
        # session was rotated: destroy old + create new
        assert mock_fs_client.destroy_session.call_count == 1
        # create_session: 1 init + 1 rotation = 2
        assert mock_fs_client.create_session.call_count == 2
        assert reader._session_id != original_sid
        # _request_count still 1 (only the recovered call counts)
        assert reader._request_count == 1

    def test_tab_crash_propagates_after_max_recoveries(self, mock_fs_client, reader, monkeypatch):
        """If every retry up to _max_recoveries crashes, the error must propagate (no infinite loop)."""
        from scrapers.base.flaresolverr_client import FlareSolverrTabCrashed

        # Speed up the test — no real sleep needed between attempts.
        monkeypatch.setattr('time.sleep', lambda *_: None)

        # _max_recoveries=3 → 1 initial + 3 retries = 4 total crashes.
        mock_fs_client.get.side_effect = [
            FlareSolverrTabCrashed('Error: tab crashed (chrome=142)')
            for _ in range(reader._max_recoveries + 1)
        ]

        with pytest.raises(FlareSolverrTabCrashed):
            reader._download_and_save('https://sofifa.com/team/9/')

    def test_cf_challenge_timeout_triggers_session_rotation_and_retry(self, mock_fs_client, reader, monkeypatch):
        """FlareSolverrCFChallengeFailed (e.g. challenge timeout) also triggers rotation+retry."""
        from scrapers.base.flaresolverr_client import FlareSolverrCFChallengeFailed

        monkeypatch.setattr('time.sleep', lambda *_: None)
        original_sid = reader._session_id
        mock_fs_client.get.side_effect = [
            FlareSolverrCFChallengeFailed('Error solving the challenge. Timeout after 90.0 seconds.'),
            {'html': '<html>recovered</html>', 'status': 200},
        ]

        result = reader._download_and_save('https://sofifa.com/player/231747/')

        assert b'recovered' in result.getvalue()
        assert mock_fs_client.destroy_session.call_count == 1
        assert mock_fs_client.create_session.call_count == 2
        assert reader._session_id != original_sid

    def test_session_rotates_every_n_requests(self, mock_fs_client, reader):
        mock_fs_client.get.return_value = {'html': '<html>X</html>', 'status': 200}
        reader._session_recreate_every = 2
        original_sid = reader._session_id

        reader._download_and_save('https://a/')  # request 1, no rotation
        sid_after_1 = reader._session_id
        reader._download_and_save('https://b/')  # request 2, rotation triggered on next call
        sid_after_2 = reader._session_id
        reader._download_and_save('https://c/')  # next call → recreate session

        assert sid_after_1 == original_sid
        assert sid_after_2 == original_sid
        # request 3: _maybe_recreate_session(count=2) destroyed old session, created new
        assert reader._session_id != original_sid
        # create_session called 2 times total (1 init + 1 rotation)
        assert mock_fs_client.create_session.call_count == 2
        # destroy was called once for the rotation
        assert mock_fs_client.destroy_session.call_count == 1


class TestReadPlayerRatingsParsing:
    """Regression for #316: the attacking attribute is labelled 'Attack position'
    on sofifa.com, NOT 'Positioning'. Upstream searches the substring
    'Positioning', which only matches 'GK Positioning', so the attacking
    `positioning` column silently captured the goalkeeper value.
    """

    _PLAYER_HTML = (
        '<html><body>'
        '<div class="profile"><h1>Test Player</h1></div>'
        '<p><span><em>80</em></span> Attack position</p>'
        '<p><span><em>9</em></span> GK Positioning</p>'
        '</body></html>'
    )

    def test_positioning_is_attack_position_not_gk(self, reader, tmp_path):
        import io as _io

        import pandas as pd

        reader.data_dir = tmp_path
        reader.versions = pd.DataFrame([{'update': 'Jun 2 2026'}], index=[260035])
        reader.get = MagicMock(
            side_effect=lambda *a, **k: _io.BytesIO(self._PLAYER_HTML.encode('utf-8'))
        )

        df = reader.read_player_ratings(player=[12345])

        row = df.iloc[0]
        # Attacking 'Attack position' (80) lands in `positioning` ...
        assert int(row['positioning']) == 80
        # ... and the goalkeeper value (9) stays in `gk_positioning`.
        assert int(row['gk_positioning']) == 9
        # The two must NOT collide (the bug made them identical at 9).
        assert int(row['positioning']) != int(row['gk_positioning'])


class TestReadTeamRatingsParsing:
    """#601: read_team_ratings must scrape only the 8 columns sofifa.com still
    renders on the FC 26 team page and never emit the 15 dead-tactics columns
    EA removed (they would otherwise land as all-NULL in Bronze).
    """

    _TEAM_HTML = (
        '<html><body><table><tbody>'
        '<tr>'
        '<td>1</td>'
        '<td><a href="/team/9/test-team/">Test Team</a></td>'
        '<td data-col="oa">85</td>'
        '<td data-col="at">84</td>'
        '<td data-col="md">83</td>'
        '<td data-col="df">82</td>'
        '<td data-col="tb">€45M</td>'
        '<td data-col="cw">€500M</td>'
        '<td data-col="ps">27</td>'
        '<td data-col="sa">26</td>'
        '</tr>'
        '</tbody></table></body></html>'
    )

    _DEAD_COLS = [
        'build_up_speed', 'build_up_dribbling', 'build_up_passing',
        'build_up_positioning', 'chance_creation_crossing',
        'chance_creation_passing', 'chance_creation_shooting',
        'chance_creation_positioning', 'defence_aggression', 'defence_pressure',
        'defence_team_width', 'defence_defender_line', 'defence_domestic_prestige',
        'international_prestige', 'whole_team_average_age',
    ]

    def _prime(self, reader, tmp_path):
        import io as _io

        import pandas as pd

        reader.data_dir = tmp_path
        reader.versions = pd.DataFrame(
            [{'fifa_edition': 'FC 26', 'update': 'Jun 2 2026'}], index=[260035]
        )
        reader.read_leagues = MagicMock(
            return_value=pd.DataFrame(
                [{'league_id': 13}], index=['ENG-Premier League']
            )
        )
        reader.get = MagicMock(
            side_effect=lambda *a, **k: _io.BytesIO(self._TEAM_HTML.encode('utf-8'))
        )

    def test_scrapes_eight_live_columns(self, reader, tmp_path):
        self._prime(reader, tmp_path)
        df = reader.read_team_ratings().reset_index()
        row = df.iloc[0]
        assert row['team'] == 'Test Team'
        assert row['overall'] == '85'
        assert row['attack'] == '84'
        assert row['midfield'] == '83'
        assert row['defence'] == '82'
        assert row['transfer_budget'] == '€45M'
        assert row['club_worth'] == '€500M'
        assert row['players'] == '27'
        assert row['starting_xi_average_age'] == '26'

    def test_dead_columns_absent(self, reader, tmp_path):
        self._prime(reader, tmp_path)
        df = reader.read_team_ratings()
        for col in self._DEAD_COLS:
            assert col not in df.columns, (
                f"dead FC-26 column {col!r} must not be scraped (#601)"
            )

    def test_url_requests_only_live_cols(self, reader, tmp_path):
        self._prime(reader, tmp_path)
        reader.read_team_ratings()
        url = reader.get.call_args.args[0]
        # honest URL: requests the 8 live cols, never the removed ones.
        assert 'showCol[]=oa' in url and 'showCol[]=sa' in url
        assert 'showCol[]=bs' not in url  # build_up_speed
        assert 'showCol[]=ip' not in url  # international_prestige
