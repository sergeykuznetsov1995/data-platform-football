"""
Unit tests for ``scrapers/fbref/parsers/finders.py::parse_match_managers``
(E2 Phase 1.5 — 2026-05).

The parser walks the FBref scorebox to extract home/away managers. Both
the linked form (``<a href="/managers/<hash>/...">Name</a>``) and the
plain-text form ("Manager: Some Name") must work.
"""

from __future__ import annotations

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from scrapers.fbref.parsers.finders import parse_match_managers


pytestmark = pytest.mark.unit


def _scorebox(home_block: str, away_block: str) -> BeautifulSoup:
    """Wrap two team-block <div>s in a synthetic scorebox."""
    html = f"""
    <html><body>
      <div class="scorebox">
        <div>
          <a href="/en/squads/aaaaaaaa/Arsenal-Stats">Arsenal</a>
          <div class="scores"><div class="score">2</div></div>
          {home_block}
        </div>
        <div>
          <a href="/en/squads/bbbbbbbb/Liverpool-Stats">Liverpool</a>
          <div class="scores"><div class="score">1</div></div>
          {away_block}
        </div>
        <div>
          <!-- trailing meta block (date / kickoff / venue) — must be ignored -->
          <div>Saturday, August 12, 2023 (15:00 BST)</div>
        </div>
      </div>
    </body></html>
    """
    return BeautifulSoup(html, "html.parser")


class TestParseMatchManagers:
    def test_both_linked_managers(self):
        """Both managers wrapped in /managers/<hash>/ links — fbref_id captured."""
        soup = _scorebox(
            '<div>Manager: <a href="/en/managers/12345678/Mikel-Arteta">Mikel Arteta</a></div>',
            '<div>Manager: <a href="/en/managers/abcdef01/Jurgen-Klopp">Jürgen Klopp</a></div>',
        )
        df = parse_match_managers(soup)
        assert df is not None
        assert len(df) == 2

        home = df[df["side"] == "home"].iloc[0]
        away = df[df["side"] == "away"].iloc[0]

        assert home["manager_name"] == "Mikel Arteta"
        assert home["manager_fbref_id"] == "12345678"
        assert home["team"] == "Arsenal"

        assert away["manager_name"] == "Jürgen Klopp"
        assert away["manager_fbref_id"] == "abcdef01"
        assert away["team"] == "Liverpool"

    def test_plain_text_manager_no_link(self):
        """Older fixtures: 'Manager: Name' as plain text → no manager_fbref_id."""
        soup = _scorebox(
            '<div>Manager: Arsène Wenger</div>',
            '<div>Manager: <a href="/en/managers/cccccccc/Brendan-Rodgers">Brendan Rodgers</a></div>',
        )
        df = parse_match_managers(soup)
        assert df is not None
        assert len(df) == 2

        home = df[df["side"] == "home"].iloc[0]
        assert home["manager_name"] == "Arsène Wenger"
        assert pd.isna(home["manager_fbref_id"])  # None in pandas<3, NaN in pandas 3 object coercion

        away = df[df["side"] == "away"].iloc[0]
        assert away["manager_fbref_id"] == "cccccccc"

    def test_missing_manager_yields_null_row(self):
        """When a team's block has no Manager line, emit a NULL row not skip."""
        soup = _scorebox(
            '<div>Manager: <a href="/en/managers/12345678/Mikel-Arteta">Mikel Arteta</a></div>',
            '<div>Captain: Virgil van Dijk</div>',
        )
        df = parse_match_managers(soup)
        assert df is not None
        # Both sides must be represented even if one has no manager.
        sides = sorted(df["side"].tolist())
        assert sides == ["away", "home"]

        away = df[df["side"] == "away"].iloc[0]
        assert pd.isna(away["manager_name"]) or away["manager_name"] is None
        assert pd.isna(away["manager_fbref_id"]) or away["manager_fbref_id"] is None

    def test_no_scorebox_returns_none(self):
        """Page without scorebox → parser returns None (caller treats as failure)."""
        soup = BeautifulSoup("<html><body>nothing here</body></html>", "html.parser")
        assert parse_match_managers(soup) is None

    def test_label_case_insensitive(self):
        """`MANAGER:` / `manager:` should both match."""
        soup = _scorebox(
            '<div>MANAGER: Mikel Arteta</div>',
            '<div>manager: Brendan Rodgers</div>',
        )
        df = parse_match_managers(soup)
        assert df is not None
        names = sorted(df["manager_name"].tolist())
        assert names == ["Brendan Rodgers", "Mikel Arteta"]

    def test_extra_whitespace_stripped(self):
        """Manager labels with surrounding whitespace are trimmed."""
        soup = _scorebox(
            '<div>  Manager:   Mikel Arteta   </div>',
            '<div>Manager: Brendan Rodgers</div>',
        )
        df = parse_match_managers(soup)
        assert df is not None
        names = sorted(df["manager_name"].tolist())
        assert names == ["Brendan Rodgers", "Mikel Arteta"]

    def test_real_fbref_scorebox_team_markup(self):
        """Real FBref 2025 markup uses <div class="scorebox_team"> blocks
        with <strong>Manager</strong>: Name (note the space before colon)
        and U+00A0 NBSP inside multi-word names. The parser must:
          - locate scorebox_team blocks (not just the first 2 child <div>s),
          - tolerate the optional space before the colon,
          - replace NBSP with regular space in the name.
        """
        html = """
        <html><body>
          <div class="scorebox">
            <div class="scorebox_team" id="sb_team_0">
              <a href="/en/squads/aaaaaaaa/Liverpool-Stats">Liverpool</a>
              <div class="scores"><div class="score">4</div></div>
              <div class="datapoint"><strong>Manager</strong>: Arne Slot</div>
              <div class="datapoint"><strong>Captain</strong>: Virgil van Dijk</div>
            </div>
            <div class="scorebox_team" id="sb_team_1">
              <a href="/en/squads/bbbbbbbb/Bournemouth-Stats">Bournemouth</a>
              <div class="scores"><div class="score">2</div></div>
              <div class="datapoint"><strong>Manager</strong>: Andoni Iraola</div>
              <div class="datapoint"><strong>Captain</strong>: Adam Smith</div>
            </div>
            <div class="scorebox_meta">
              <div>Friday August 15, 2025</div>
            </div>
          </div>
        </body></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        df = parse_match_managers(soup)
        assert df is not None
        assert len(df) == 2

        home = df[df["side"] == "home"].iloc[0]
        away = df[df["side"] == "away"].iloc[0]

        assert home["team"] == "Liverpool"
        assert home["manager_name"] == "Arne Slot", (
            f"NBSP must be normalised to ASCII space, got "
            f"{home['manager_name']!r}"
        )
        assert pd.isna(home["manager_fbref_id"])  # None in pandas<3, NaN in pandas 3 object coercion

        assert away["team"] == "Bournemouth"
        assert away["manager_name"] == "Andoni Iraola"
        assert away["manager_fbref_id"] is None
