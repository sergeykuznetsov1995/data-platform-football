"""Private host resources for WhoScored tests; production defaults stay intact."""

import pytest


@pytest.fixture(autouse=True)
def whoscored_test_locks(request, monkeypatch):
    if "whoscored" not in request.node.path.name:
        return
    root = request.getfixturevalue("tmp_path_factory").mktemp("whoscored-locks")
    for variable, name in (
        ("WHOSCORED_LOCK_DIR", "commit-locks"),
        ("WHOSCORED_RAW_LOCK_DIR", "raw-locks"),
    ):
        directory = root / name
        directory.mkdir(mode=0o700)
        monkeypatch.setenv(variable, str(directory))


@pytest.fixture(autouse=True)
def transfermarkt_test_writer_lock(request, monkeypatch):
    """Keep the Transfermarkt writer lock off the production log volume.

    The writer defaults to /opt/airflow/logs, which unprivileged hosts cannot
    write; tests that need a specific lock still override this per test.
    """
    if "transfermarkt" not in request.node.path.name:
        return
    lock = request.getfixturevalue("tmp_path_factory").mktemp("tm-lock") / "writer.lock"
    monkeypatch.setenv("TM_WRITER_LOCK_PATH", str(lock))


@pytest.fixture(autouse=True)
def sofascore_published_status(request, monkeypatch):
    """No-op SofaScore captures see every status row published (#1957).

    The probe reads Bronze over Trino; tests of the repair override it.
    """
    if "sofascore" not in request.node.path.name:
        return
    from dags.scripts import run_sofascore_scraper

    monkeypatch.setattr(
        run_sofascore_scraper, "_published_status_ids",
        lambda _league, _season, match_ids: {str(match_id) for match_id in match_ids},
    )
