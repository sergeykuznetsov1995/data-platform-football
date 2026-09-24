"""Smoke checks for the ClubElo site fixtures captured on 2026-09-24 (#1461).

No parser yet: the tests only prove every fixture is present, intact and still
carries the layout markers the future parsers rely on.
"""

import gzip
import hashlib
import json
import re
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parents[2] / "fixtures" / "clubelo" / "20260924"
MANIFEST = json.loads((FIXTURE_DIR / "manifest.json").read_text())["files"]

LIST_PAGES = ["Ranking", "Results", "Fixtures"]
CLUB_PAGES = sorted(p.name.split(".", 1)[0] for p in FIXTURE_DIR.glob("club_*.html.gz"))
H1_DATE = re.compile(r'<h1><a href="/\d{4}-\d{2}-\d{2}/')


def _text(name: str) -> str:
    path = FIXTURE_DIR / name
    if name.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            return fh.read()
    return path.read_text(encoding="utf-8")


def test_manifest_lists_existing_files_with_matching_sha256():
    on_disk = {p.name for p in FIXTURE_DIR.iterdir()} - {"manifest.json", "README.md"}
    assert set(MANIFEST) == on_disk
    for name, meta in MANIFEST.items():
        assert hashlib.sha256((FIXTURE_DIR / name).read_bytes()).hexdigest() == meta["sha256"], name


def test_fixture_directory_is_at_most_1_mb():
    assert sum(p.stat().st_size for p in FIXTURE_DIR.iterdir()) <= 1024 * 1024


def test_club_pages_cover_the_review_samples():
    assert CLUB_PAGES == ["club_Arsenal", "club_lsapi-4199", "club_riverplate", "club_santos-fc_2"]


@pytest.mark.parametrize("page", LIST_PAGES + CLUB_PAGES)
def test_page_has_rating_date_h1(page):
    assert H1_DATE.search(_text(f"{page}.html.gz")), page


@pytest.mark.parametrize("page", LIST_PAGES)
def test_list_page_has_creation_stamp(page):
    # Club pages carry no "Page created on" stamp (checked 2026-09-24).
    assert "Page created on" in _text(f"{page}.html.gz")


def test_ranking_markers():
    html = _text("Ranking.html.gz")
    assert "eloData = [" in html
    assert "var vegaJson" in html


@pytest.mark.parametrize("page", CLUB_PAGES)
def test_club_page_markers(page):
    html = _text(f"{page}.html.gz")
    assert "var vegaJson" in html
    assert "Post-Game" in html and "New Elo" in html


def test_results_markers():
    html = _text("Results.html.gz")
    assert "Post-Game" in html and ">Home<" in html


def test_login_says_registration_is_closed():
    assert "registration is not available" in _text("login.html.gz")


def test_server_error_body():
    assert "Server Error (500)" in _text("Arsenal_Results.500.html")


@pytest.mark.parametrize(
    "name", ["lsapi-2483.302.headers.txt", "Arsenal_Results.500.headers.txt"]
)
def test_header_only_responses_match_manifest(name):
    lines = _text(name).splitlines()
    assert int(lines[0].split()[1]) == MANIFEST[name]["http_status"]
    if MANIFEST[name]["http_status"] in (301, 302):
        assert any(line.lower().startswith("location:") for line in lines)


def test_headers_have_no_cookies():
    for path in FIXTURE_DIR.glob("*.headers.txt"):
        assert "set-cookie" not in path.read_text().lower(), path.name
