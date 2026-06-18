"""
Unit tests for ``scrapers/fbref/parsers/finders.py::parse_match_officials``
(issue #613).

The parser reads the FBref match-page ``scorebox_meta`` Officials block, where
each official is a ``<span style="display:inline-block">Name (Role)</span>``
with roles pre-numbered by FBref — (Referee) (AR1) (AR2) (4th) (VAR). The
markup below mirrors a live APL match page (Liverpool–Bournemouth, 2026-06-16).
"""

from __future__ import annotations

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from scrapers.fbref.parsers.finders import parse_match_officials


pytestmark = pytest.mark.unit


def _span(text: str) -> str:
    return f'<span style="display:inline-block">{text}</span>'


def _meta(inner: str) -> BeautifulSoup:
    """Wrap an Officials ``<small>`` body in a realistic scorebox_meta div."""
    html = f"""
    <html><body>
      <div class="scorebox_meta">
        <div><strong><small>Venue</small></strong>: <small>Anfield, Liverpool</small></div>
        <div>
          <strong><small>Officials</small></strong>
          :
          <small>{inner}</small>
        </div>
      </div>
    </body></html>
    """
    return BeautifulSoup(html, "html.parser")


class TestParseMatchOfficials:
    def test_full_crew(self):
        """Real FBref markup: Referee/AR1/AR2/4th/VAR → 5 wide columns."""
        soup = _meta(
            _span("Anthony Taylor (Referee)") + " · "
            + _span("Gary Beswick (AR1)") + " · "
            + _span("Craig Taylor (AR2)") + " · "
            + _span("Farai Hallam (4th)") + " · "
            + _span("Michael Oliver (VAR)")
        )
        df = parse_match_officials(soup)
        assert df is not None
        assert len(df) == 1

        row = df.iloc[0]
        assert row["referee"] == "Anthony Taylor"
        assert row["ar1"] == "Gary Beswick"
        assert row["ar2"] == "Craig Taylor"
        assert row["fourth_official"] == "Farai Hallam"
        assert row["var"] == "Michael Oliver"

    def test_missing_var_and_fourth_stay_null(self):
        """Fixture with only referee + 2 ARs → 4th/var NULL, row not dropped."""
        soup = _meta(
            _span("Sam Barrott (Referee)") + " · "
            + _span("Dan Cook (AR1)") + " · "
            + _span("Nick Hopton (AR2)")
        )
        df = parse_match_officials(soup)
        assert df is not None
        row = df.iloc[0]
        assert row["referee"] == "Sam Barrott"
        assert row["ar1"] == "Dan Cook"
        assert row["ar2"] == "Nick Hopton"
        assert pd.isna(row["fourth_official"]) or row["fourth_official"] is None
        assert pd.isna(row["var"]) or row["var"] is None

    def test_official_name_as_link(self):
        """Official name wrapped in an <a> link — text extracted, role mapped."""
        soup = _meta(
            _span('<a href="/en/referees/abcd1234/Michael-Oliver">Michael Oliver</a> (Referee)')
        )
        df = parse_match_officials(soup)
        assert df is not None
        assert df.iloc[0]["referee"] == "Michael Oliver"

    def test_nbsp_normalised(self):
        """U+00A0 inside a name is normalised to a regular ASCII space."""
        soup = _meta(_span("Anthony Taylor (Referee)"))
        df = parse_match_officials(soup)
        assert df is not None
        assert df.iloc[0]["referee"] == "Anthony Taylor"

    def test_label_case_insensitive(self):
        """Role labels are matched case-insensitively (referee/REFEREE)."""
        soup = _meta(_span("John Brooks (referee)") + " · " + _span("Tim Wood (var)"))
        df = parse_match_officials(soup)
        assert df is not None
        assert df.iloc[0]["referee"] == "John Brooks"
        assert df.iloc[0]["var"] == "Tim Wood"

    def test_unknown_role_ignored(self):
        """Unknown roles (e.g. AVAR on a cup tie) fall through; known ones map."""
        soup = _meta(
            _span("John Brooks (Referee)") + " · " + _span("Someone Else (AVAR)")
        )
        df = parse_match_officials(soup)
        assert df is not None
        assert df.iloc[0]["referee"] == "John Brooks"
        assert pd.isna(df.iloc[0]["var"]) or df.iloc[0]["var"] is None

    def test_no_officials_block_returns_none(self):
        """Page without an Officials block → None (parse failure, not empty)."""
        soup = BeautifulSoup(
            '<html><body><div class="scorebox_meta">'
            '<div><strong><small>Venue</small></strong>: <small>Anfield</small></div>'
            "</div></body></html>",
            "html.parser",
        )
        assert parse_match_officials(soup) is None
