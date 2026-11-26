"""Source-backed typed match metadata and lineup formations (#1331)."""

import gzip
from pathlib import Path

import pandas as pd
import pytest

from scrapers.fbref import typed_bronze as typed
from scrapers.fbref.match_parser import DatasetStatus
from tests.unit.scrapers.test_fbref_typed_bronze import RecordingManager


MATCHES = Path(__file__).resolve().parents[2] / "fixtures" / "fbref" / "matches"
CONTEXT = typed.TypedSourceContext("9", "2025-2026")


def _parse_meta(html):
    return typed.parse_match_html(
        html, match_id="643d26fd", context=CONTEXT, enabled_datasets={"match_meta"}
    )


def test_real_match_persists_native_ids_scores_captains_venue_and_formations():
    manager = RecordingManager()
    adapter = typed.FBrefTypedBronzeAdapter(typed.FBrefTypedBronzeWriter(manager))
    parsed, counts = adapter.ingest_match_html(
        gzip.decompress((MATCHES / "643d26fd.html.gz").read_bytes()),
        match_id="643d26fd",
        context=CONTEXT,
        run_id="fixture",
        target_identity="fbref:match:643d26fd",
    )

    assert parsed.has_score and not parsed.has_errors
    assert counts["match_meta"] == 1
    frames = {write["table"]: write["frame"] for write in manager.writes}
    meta = frames["fbref_match_meta"].iloc[0]
    assert (meta.match_id, meta.source_competition_id, meta.source_season_id) == (
        "643d26fd",
        "9",
        "2025-2026",
    )
    assert (meta.home_team_id, meta.home_team, meta.home_goals) == (
        "19538871",
        "Manchester United",
        0,
    )
    assert (meta.away_team_id, meta.away_team, meta.away_goals) == (
        "18bb7c10",
        "Arsenal",
        1,
    )
    assert (meta.home_captain_id, meta.home_captain) == ("507c7bdf", "Bruno Fernandes")
    assert (meta.away_captain_id, meta.away_captain) == ("79300479", "Martin Ødegaard")
    assert (meta.venue, meta.attendance) == ("Old Trafford, Manchester", 73475)
    assert pd.isna(meta.home_xg) and pd.isna(meta.away_xg)
    assert manager.columns["fbref_match_meta"]["home_xg"] == "DOUBLE"
    assert manager.writes[-1]["table"] == "fbref_dataset_availability"
    lineups = frames["fbref_lineups"]
    assert lineups.groupby("team").formation.unique().apply(list).to_dict() == {
        "Manchester Utd": ["3-4-3"],
        "Arsenal": ["4-3-3"],
    }


def test_named_sides_survive_reordered_dom_and_missing_optional_fields():
    html = """
    <div class="scorebox">
      <div id="sb_team_1" class="scorebox_team">
        <strong><a href="/en/squads/22222222/Team-B">B</a></strong>
        <div class="scores"><div class="score">0</div><div class="score_xg">0.0</div></div>
        <div class="datapoint"><strong>Captain</strong>: Named Player</div>
      </div>
      <div id="sb_team_0" class="scorebox_team">
        <strong><a href="/en/squads/11111111/Team-A">A</a></strong>
        <div class="scores"><div class="score">0</div><div class="score_xg">2.3</div></div>
        <div class="datapoint"><strong>Manager</strong>: Someone Else</div>
        <div class="datapoint"><strong>Captain</strong>: —</div>
      </div>
      <div class="scorebox_meta"><div><strong>Attendance</strong>: 0</div></div>
    </div>
    """
    parsed = _parse_meta(html)
    assert parsed.has_score
    row = parsed.datasets["match_meta"].frame.iloc[0]
    assert (row.home_team_id, row.away_team_id) == ("11111111", "22222222")
    assert (row.home_xg, row.away_xg, row.attendance) == (2.3, 0.0, 0)
    assert (row.home_goals, row.away_goals) == (0, 0)
    assert pd.isna(row.home_captain) and pd.isna(row.home_captain_id)
    assert row.away_captain == "Named Player" and pd.isna(row.away_captain_id)
    assert pd.isna(row.venue)


def _shared_captain_page(labels, lineups="", home_captain=""):
    return f"""
    <div class="scorebox">
      <div id="sb_team_0"><a href="/en/squads/11111111/A">Home</a>{home_captain}</div>
      <div id="sb_team_1"><a href="/en/squads/22222222/B">Away</a></div>
      <div class="scorebox_meta">{labels}</div>
    </div>{lineups}"""


def _captain_label(player_id, name, team=""):
    return f'<div><strong>Captain</strong>: <a href="/en/players/{player_id}/player">{name}</a>{team}</div>'


def _lineup(side, player_id):
    return (
        f'<div class="lineup" id="{side}"><table><tr><td>9</td>'
        f'<td><a href="/en/players/{player_id}/listed">Listed player</a></td></tr></table></div>'
    )


def test_shared_captains_use_lineup_membership_and_explicit_team_id_instead_of_label_order():
    labels = _captain_label("bbbbbbbb", "Away player") + _captain_label(
        "aaaaaaaa", "Home player"
    )
    # The shared labels and the lineup containers both list the away side first.
    lineups = _lineup("b", "bbbbbbbb") + _lineup("a", "aaaaaaaa")
    row = (
        _parse_meta(_shared_captain_page(labels, lineups))
        .datasets["match_meta"]
        .frame.iloc[0]
    )
    assert (row.home_captain, row.home_captain_id) == ("Home player", "aaaaaaaa")
    assert (row.away_captain, row.away_captain_id) == ("Away player", "bbbbbbbb")

    # A source-owned squad link identifies the side even without a lineup.
    label = _captain_label(
        "bbbbbbbb", "Away player", ' (<a href="/en/squads/22222222/B">Away</a>)'
    )
    row = _parse_meta(_shared_captain_page(label)).datasets["match_meta"].frame.iloc[0]
    assert (row.away_captain, row.away_captain_id) == ("Away player", "bbbbbbbb")
    assert pd.isna(row.home_captain_id)


@pytest.mark.parametrize(
    ("labels", "lineups"),
    [
        (_captain_label("aaaaaaaa", "Unknown side"), ""),
        (
            "<div><strong>Captain</strong>: Listed player</div>",
            _lineup("a", "aaaaaaaa"),
        ),
        (
            _captain_label("aaaaaaaa", "Both sides"),
            _lineup("a", "aaaaaaaa") + _lineup("b", "aaaaaaaa"),
        ),
        (
            _captain_label(
                "aaaaaaaa", "Conflicting", ' (<a href="/en/squads/22222222/B">Away</a>)'
            ),
            _lineup("a", "aaaaaaaa"),
        ),
        (
            _captain_label("aaaaaaaa", "First") + _captain_label("bbbbbbbb", "Second"),
            _lineup("a", "aaaaaaaa") + _lineup("a", "bbbbbbbb"),
        ),
    ],
)
def test_ambiguous_shared_captains_remain_unknown(labels, lineups):
    row = (
        _parse_meta(_shared_captain_page(labels, lineups))
        .datasets["match_meta"]
        .frame.iloc[0]
    )
    assert (
        row[["home_captain_id", "away_captain_id", "home_captain", "away_captain"]]
        .isna()
        .all()
    )


def test_shared_captain_preserves_existing_side_specific_label():
    own = "<div><strong>Captain</strong>: Published home captain</div>"
    page = _shared_captain_page(
        _captain_label("aaaaaaaa", "Other"), _lineup("a", "aaaaaaaa"), home_captain=own
    )
    row = _parse_meta(page).datasets["match_meta"].frame.iloc[0]
    assert row.home_captain == "Published home captain" and pd.isna(row.home_captain_id)


def test_shared_captain_respects_conflicting_side_specific_player_id():
    label = _captain_label("aaaaaaaa", "Home captain")
    page = _shared_captain_page(label, _lineup("b", "aaaaaaaa"), home_captain=label)
    row = _parse_meta(page).datasets["match_meta"].frame.iloc[0]
    assert (row.home_captain_id, row.home_captain) == ("aaaaaaaa", "Home captain")
    assert pd.isna(row.away_captain_id) and pd.isna(row.away_captain)


@pytest.mark.parametrize("in_comment", [False, True])
def test_shared_captain_uses_source_team_id_in_published_player_table(in_comment):
    table = """<table id="stats_22222222_summary"><tbody><tr><th data-stat="player">
      <a href="/en/players/bbbbbbbb/P">Listed</a></th></tr></tbody></table>"""
    page = _shared_captain_page(
        _captain_label("bbbbbbbb", "Away player"),
        f"<!--{table}-->" if in_comment else table,
    )
    row = _parse_meta(page).datasets["match_meta"].frame.iloc[0]
    assert (row.away_captain, row.away_captain_id) == ("Away player", "bbbbbbbb")
    assert pd.isna(row.home_captain_id)


def test_unscored_legacy_preview_preserves_unknown_goals_and_numeric_schema():
    html = """
    <div class="scorebox">
      <div><strong><a href="/en/squads/11111111/Team-A">A</a></strong></div>
      <div><strong><a href="/en/squads/22222222/Team-B">B</a></strong></div>
    </div>
    """
    manager = RecordingManager()
    adapter = typed.FBrefTypedBronzeAdapter(typed.FBrefTypedBronzeWriter(manager))
    result, counts = adapter.ingest_match_html(
        html,
        match_id="a1234567",
        context=CONTEXT,
        run_id="preview",
        target_identity="fbref:match:a1234567",
        enabled_datasets={"match_meta"},
    )
    assert counts == {"match_meta": 1}
    assert not result.has_score and not result.has_errors
    row = result.datasets["match_meta"].frame.iloc[0]
    assert (row.home_team, row.away_team) == ("A", "B")
    assert (
        pd.isna(row.home_goals) and pd.isna(row.away_goals) and pd.isna(row.attendance)
    )
    cols = manager.columns["fbref_match_meta"]
    assert (cols["home_goals"], cols["attendance"], cols["away_xg"]) == (
        "BIGINT",
        "BIGINT",
        "DOUBLE",
    )


def test_partial_side_ids_preserve_known_away_team_without_shifting_to_home():
    away = """
    <div id="sb_team_1" class="scorebox_team">
      <strong><a href="/en/squads/22222222/G">Guests</a></strong>
    </div>
    """
    home = """
    <div class="scorebox_team">
      <strong><a href="/en/squads/11111111/H">Hosts</a></strong>
    </div>
    """
    meta = _parse_meta('<div class="scorebox">' + away + "</div>").datasets[
        "match_meta"
    ]
    assert meta.frame.iloc[0].away_team_id == "22222222" and pd.isna(
        meta.frame.iloc[0].home_team_id
    )
    meta = _parse_meta('<div class="scorebox">' + away + home + "</div>").datasets[
        "match_meta"
    ]
    assert (meta.frame.iloc[0].home_team_id, meta.frame.iloc[0].away_team_id) == (
        "11111111",
        "22222222",
    )


@pytest.mark.parametrize(
    ("html", "status", "reason"),
    [
        ("<html/>", DatasetStatus.NOT_APPLICABLE, "source_container_not_published"),
        (
            '<div class="scorebox"><p>Unknown layout</p></div>',
            DatasetStatus.ERROR,
            "source_container_unparsed",
        ),
        (
            '<div class="scorebox"><article><i class="score">3</i><i class="score">2</i></article>'
            '<div class="scorebox_meta"><div><strong>Venue</strong>: Here</div></div></div>',
            DatasetStatus.ERROR,
            "source_container_unparsed",
        ),
    ],
)
def test_missing_or_unrecognized_scorebox_never_invents_metadata(html, status, reason):
    meta = _parse_meta(html).datasets["match_meta"]
    assert (meta.status, meta.reason, meta.row_count) == (status, reason, 0)


def test_lineup_without_formation_keeps_full_team_name_and_null_formation():
    html = """
    <div class="lineup" id="a"><table><tr><th>Arsenal (U21)</th></tr>
      <tr><td>20</td><td><a href="/en/players/aaaaaaaa/One">One</a></td></tr>
    </table></div>
    """
    parsed = typed.parse_match_html(
        html,
        match_id="a1234567",
        context=CONTEXT,
        enabled_datasets={"lineups"},
    )
    row = parsed.datasets["lineups"].frame.iloc[0]
    assert row.team == "Arsenal (U21)"
    assert pd.isna(row.formation)
