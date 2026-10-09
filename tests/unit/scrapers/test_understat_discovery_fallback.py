"""Calendar fallback must retain only the last schema-validated registry."""

from datetime import date
import json
import errno
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from scrapers.understat.catalog import LEAGUES, UnderstatCatalog
from scrapers.understat.client import UnderstatPayloadError, is_retryable_error


@pytest.mark.parametrize("error", [
    OSError(errno.EROFS, "read-only filesystem"),
    NotADirectoryError(errno.ENOTDIR, "bad directory"),
    IsADirectoryError(errno.EISDIR, "bad file"),
    OSError("unknown OS error"),
])
def test_permanent_os_errors_do_not_retry(error):
    assert not is_retryable_error(error)


def test_invalid_url_and_certificate_errors_do_not_retry():
    from requests.exceptions import InvalidURL, SSLError
    assert not is_retryable_error(InvalidURL("bad URL"))
    assert not is_retryable_error(SSLError("certificate verify failed"))


@pytest.mark.parametrize("error", [
    OSError(errno.ECONNRESET, "reset"),
    OSError(errno.EAGAIN, "temporarily unavailable"),
    TimeoutError("timeout"),
])
def test_known_temporary_os_errors_retry(error):
    assert is_retryable_error(error)


def registry():
    return {"stat": [
        dict(league=item.source_labels[0], league_id=item.source_league_id,
             h=1, a=1, hxg=1, axg=1, year=2026, month=8, matches=1)
        for item in LEAGUES
    ]}


def test_invalid_live_response_cannot_replace_last_valid_registry(tmp_path):
    client = SimpleNamespace(cache_dir=tmp_path,
                             get_stat_data=Mock(return_value=registry()))
    catalog = UnderstatCatalog(client, today=date(2026, 10, 5))
    catalog.discover_scopes()
    saved = (tmp_path / "stat.last-success.json").read_bytes()
    client.get_stat_data.return_value = {"stat": []}
    with pytest.raises(UnderstatPayloadError):
        catalog.discover_scopes()
    assert (tmp_path / "stat.last-success.json").read_bytes() == saved
    scopes = catalog.calendar_scopes()
    assert len(scopes) == 12
    assert all(scope.discovered == (scope.source_season_id == 2026) for scope in scopes)
    assert client.get_stat_data.call_count == 2  # fallback performs no request
    assert not list(tmp_path.glob(".stat.last-success*.tmp"))


@pytest.mark.parametrize("today, expected", [
    (date(2026, 6, 30), {2024, 2025, 2026}),
    (date(2026, 7, 1), {2025, 2026}),
])
def test_calendar_fallback_without_registry_preserves_rollover(tmp_path, today, expected):
    client = SimpleNamespace(cache_dir=tmp_path, get_stat_data=Mock())
    scopes = UnderstatCatalog(client, today=today).calendar_scopes()
    assert {scope.source_season_id for scope in scopes} == expected
    assert len(scopes) == len(LEAGUES) * len(expected)
    assert not any(scope.discovered for scope in scopes)
    client.get_stat_data.assert_not_called()


@pytest.mark.parametrize("contents", ["{", json.dumps({"stat": []})])
def test_corrupt_last_valid_registry_fails_closed(tmp_path, contents):
    (tmp_path / "stat.last-success.json").write_text(contents)
    with pytest.raises(UnderstatPayloadError):
        UnderstatCatalog(SimpleNamespace(cache_dir=tmp_path)).calendar_scopes()
