import gzip
from pathlib import Path

import pytest
from bs4 import BeautifulSoup, Comment

from scrapers.fbref.page_document import (
    Availability,
    parse_page_document,
    response_owns_target_page,
)


TABLELESS_PLAYER_PROFILE = """
<html><head>
  <link rel="canonical"
    href="https://fbref.com/en/players/406c5597/Naime-Said-Mchindra">
  <meta property="og:url"
    content="https://fbref.com/en/players/406c5597/Naime-Said-Mchindra">
  <meta property="og:type" content="Athlete">
</head><body>
  <div id="meta"><h1>Naime Said Mchindra</h1>
    <p>Position: GK</p><p>Born: May 28, 2005</p>
  </div>
</body></html>
"""


def test_zero_table_competition_is_deferred_to_semantic_contract():
    page = parse_page_document(
        "<html><main><h1>Competition</h1></main></html>",
        target_id="fbref:competition:zero",
        page_kind="competition",
    )

    assert page.tables == ()
    assert page.errors == ()

    broken_profile = parse_page_document(
        "<html><main><h1>Player</h1></main></html>",
        target_id="fbref:player:zero",
        page_kind="player",
    )
    assert broken_profile.errors == ("page_contract:no_tables",)


def test_verified_zero_table_player_profile_is_valid_empty_evidence():
    page = parse_page_document(
        TABLELESS_PLAYER_PROFILE,
        target_id="fbref:player:406c5597",
        page_kind="player",
        source_ids={"player_id": "406c5597"},
    )

    assert page.tables == ()
    assert page.errors == ()


@pytest.mark.parametrize(
    "html",
    [
        TABLELESS_PLAYER_PROFILE.replace("406c5597", "aaaaaaaa", 1),
        TABLELESS_PLAYER_PROFILE.replace(
            'property="og:url"', 'property="og:wrong"'
        ),
        TABLELESS_PLAYER_PROFILE.replace("Athlete", "Website"),
        TABLELESS_PLAYER_PROFILE.replace(
            "Naime-Said-Mchindra",
            "matchlogs/2025/summary/Naime-Said-Mchindra",
        ),
        TABLELESS_PLAYER_PROFILE.replace("id=\"meta\"", "id=\"content\""),
        "<html><head><title>Just a moment...</title></head></html>",
    ],
)
def test_unverified_zero_table_player_shell_still_fails(html):
    page = parse_page_document(
        html,
        target_id="fbref:player:406c5597",
        page_kind="player",
        source_ids={"player_id": "406c5597"},
    )

    assert page.tables == ()
    assert page.errors == ("page_contract:no_tables",)


def test_zero_table_player_requires_source_owned_identity():
    page = parse_page_document(
        TABLELESS_PLAYER_PROFILE,
        target_id="fbref:player:406c5597",
        page_kind="player",
    )

    assert page.errors == ("page_contract:no_tables",)


def test_empty_table_evidence_and_new_material_table_are_preserved():
    empty = parse_page_document(
        """
        <table id="stats_empty"><thead><tr>
          <th data-stat="player">Player</th>
        </tr></thead><tbody></tbody></table>
        """,
        target_id="fbref:squad:empty",
        page_kind="squad",
    )
    novel = parse_page_document(
        """
        <table id="brand_new"><tr><th data-stat="new">New</th></tr>
          <tr><td data-stat="new">value</td></tr></table>
        """,
        target_id="fbref:squad:novel",
        page_kind="squad",
    )

    assert empty.errors == ()
    assert empty.tables[0].availability == Availability.EMPTY
    assert novel.errors == ()
    assert novel.tables[0].availability == Availability.UNKNOWN


def test_required_schedule_table_cannot_be_replaced_by_unrelated_markup():
    page = parse_page_document(
        """
        <table id="advertisement"><tr><th data-stat="name">Name</th></tr>
          <tr><td data-stat="name">Sponsor</td></tr></table>
        """,
        target_id="fbref:schedule:9:2025",
        page_kind="schedule",
    )

    assert page.tables[0].availability == Availability.UNKNOWN
    assert page.errors == (
        "page_contract:required_table_missing:sched",
    )


def test_inventories_dom_and_every_table_inside_comments_losslessly():
    html = """
    <table id="stats_standard">
      <thead>
        <tr><th rowspan="2">Player</th><th colspan="2">Performance</th></tr>
        <tr><th>Goals</th><th>Assists</th></tr>
      </thead>
      <tbody><tr>
        <th data-stat="player"><a href="/en/players/abcd1234/Test">Test</a></th>
        <td data-stat="goals">2</td><td data-stat="assists">1</td>
      </tr></tbody>
    </table>
    <!--
      <table id="sched_group"><tr><th data-stat="round">Round</th></tr>
        <tr><td data-stat="round">Group A</td></tr></table>
      <table id="brand_new_data"><tr><th data-stat="novel">Novel</th></tr>
        <tr><td data-stat="novel"><a href="/en/squads/ffff0000/X">x</a></td></tr></table>
    -->
    """

    page = parse_page_document(
        html, target_id="fbref:season:9:2025", page_kind="season"
    )

    assert [table.table_id for table in page.tables] == [
        "stats_standard", "sched_group", "brand_new_data"
    ]
    assert [table.source_location for table in page.tables] == [
        "dom", "comment:0", "comment:0"
    ]
    assert page.tables[0].availability == Availability.AVAILABLE
    assert page.tables[2].availability == Availability.UNKNOWN
    assert page.tables[0].rows[0].cells[0].entity_ids == {
        "player_id": "abcd1234"
    }
    assert page.tables[0].rows[0].cells[1].raw_header_path == (
        "Performance", "Goals"
    )
    assert page.tables[2].rows[0].cells[0].entity_ids == {
        "squad_id": "ffff0000"
    }


def test_statuses_cover_duplicate_empty_layout_and_restricted():
    duplicate = """
      <table id="sched_all"><tr><th data-stat="date">Date</th></tr>
      <tr><td data-stat="date">2026-01-01</td></tr></table>
    """
    html = f"""
      {duplicate}
      <!-- {duplicate} -->
      <table id="stats_empty"><thead><tr><th data-stat="player">Player</th></tr></thead><tbody></tbody></table>
      <table id="layout"><tr><td><div>box</div></td></tr></table>
      <div>Data is not available for this competition
        <table id="stats_restricted"><thead><tr><th data-stat="player">Player</th></tr></thead></table>
      </div>
    """

    page = parse_page_document(
        html, target_id="fbref:test:statuses", page_kind="profile"
    )
    by_id = {table.table_id: table for table in page.tables}

    schedule_tables = [t for t in page.tables if t.table_id == "sched_all"]
    assert len(schedule_tables) == 2
    assert schedule_tables[0].availability == Availability.AVAILABLE
    assert schedule_tables[1].availability == Availability.DUPLICATE
    assert schedule_tables[1].duplicate_of == schedule_tables[0].table_instance_id
    assert by_id["stats_empty"].availability == Availability.EMPTY
    assert by_id["layout"].availability == Availability.LAYOUT_ONLY
    assert by_id["stats_restricted"].availability == Availability.RESTRICTED


def test_anonymous_table_id_and_identity_are_deterministic():
    html = """
    <table><tr><th data-stat="player">Player</th></tr>
      <tr><td data-stat="player">A</td></tr></table>
    """
    first = parse_page_document(html, target_id="t", page_kind="player")
    second = parse_page_document(html, target_id="t", page_kind="player")

    assert first.tables[0].table_id.startswith("anon_")
    assert first.tables[0].table_id == second.tables[0].table_id
    assert first.tables[0].table_instance_id == second.tables[0].table_instance_id
    assert first.tables[0].rows[0].row_id == second.tables[0].rows[0].row_id


def test_real_match_table_ids_survive_formation_order_and_comment_changes():
    fixture = Path(__file__).resolve().parents[2] / "fixtures/fbref/matches/2185fc64.html.gz"
    original = gzip.decompress(fixture.read_bytes()).decode()
    soup = BeautifulSoup(original, "html.parser")
    home = soup.select_one("div.lineup#a")
    home.th.string = "Chelsea (4-4-2)"
    home.table.replace_with(Comment(str(home.table)))
    soup.select_one("div.lineup#b").insert_before(soup.select_one("div#team_stats").extract())

    def match_tables(html):
        page = parse_page_document(html, target_id="fbref:match:2185fc64", page_kind="match")
        assert not page.errors
        return {table.table_id: table for table in page.tables}

    before, after = match_tables(original), match_tables(str(soup))
    assert before.keys() == after.keys()
    for name in ("lineup_a", "lineup_b", "team_stats"):
        assert before[name].dataset == after[name].dataset == f"table:{name}"
        assert before[name].availability == after[name].availability == Availability.AVAILABLE
        assert [r.row_id for r in before[name].rows] == [r.row_id for r in after[name].rows]
        # The snapshots themselves remain addressable for audit and replay.
        assert before[name].table_instance_id != after[name].table_instance_id
    assert before["lineup_a"].header_paths != after["lineup_a"].header_paths
    assert before["lineup_a"].source_location == "dom"
    assert after["lineup_a"].source_location.startswith("comment:")

    # Reorder this saved match's seven source tables inside a single comment.
    # Keep explicit FBref lineup/team-stat containers around their anonymous tables.
    tables = BeautifulSoup(original, "html.parser").find_all("table")
    fragment = "".join(str(t if t.get("id") else t.parent) for t in reversed(tables))
    in_comment = match_tables("<!-- unrelated -->" + "<!--" + fragment + "-->")
    assert len(in_comment) == len(tables) == 7
    assert in_comment.keys() == before.keys()
    for name, table in in_comment.items():
        assert table.source_location == "comment:1"
        assert table.dataset == before[name].dataset
        assert table.rows == before[name].rows


def test_named_captioned_and_anonymous_table_datasets_do_not_depend_on_order():
    named = '<table id="nations"><tr><th>Nation</th></tr><tr><td>MX</td></tr></table>'
    captioned = "<table><caption>Group A</caption><tr><th>Pts</th></tr><tr><td>3</td></tr></table>"
    anonymous = "<table><tr><th>City</th></tr><tr><td>Rome</td></tr></table>"

    def identities(html):
        page = parse_page_document(html, target_id="sample", page_kind="season")
        return {t.rows[0].cells[0].raw_value: (t.table_id, t.dataset) for t in page.tables}

    first = identities(named + "<!-- " + captioned + anonymous + " -->")
    second = identities(anonymous + "<!-- arbitrary comment -->" + captioned + "<!-- " + named + " -->")
    assert first == second
    assert first["MX"] == ("nations", "table:nations")
    assert len({name for _, name in first.values()}) == 3
    assert identities(captioned.replace("Pts", "Points"))["3"] == first["3"]


def test_duplicate_source_ids_keep_caption_identity_after_order_and_value_changes():
    def standing(caption, points):
        return (
            '<table id="results2026311_overall"><caption>' + caption
            + '</caption><tr><th data-stat="rank">Rank</th><th data-stat="team">Team</th>'
            + '<th data-stat="games">MP</th></tr><tr><td>1</td><td>Atlas</td>'
            + f"<td>{points}</td></tr></table>"
        )

    def by_caption(html):
        page = parse_page_document(html, target_id="fbref:season:31:2026", page_kind="season")
        assert all(t.availability == Availability.AVAILABLE for t in page.tables)
        return {t.caption: t.dataset for t in page.tables}

    a, b = standing("Group A", 3), standing("Group B", 1)
    before = by_caption(a + "<!--" + b + "-->")
    after = by_caption(b + "<!-- Other -->" + "<!--" + standing("Group A", 5) + "-->")
    assert before == after
    assert len(set(before.values())) == 2


def test_literal_source_id_cannot_collide_with_a_manifest_instance_suffix():
    def table(table_id):
        return f'<table id="{table_id}"><tr><th>Key</th></tr><tr><td>A</td></tr></table>'

    page = parse_page_document(
        table("nations") * 2 + table("nations:instance:2"),
        target_id="sample",
        page_kind="season",
    )
    assert {t.dataset for t in page.tables} == {
        "table:nations", "table:nations:instance:2", "table:nations%3Ainstance%3A2",
    }


def test_standings_id_alone_does_not_classify_unrecognised_rows():
    page = parse_page_document(
        '<table id="results2026311_overall"><tr><th>Advertisement</th></tr>'
        "<tr><td>One</td></tr></table>", target_id="t", page_kind="season",
    )
    assert page.tables[0].availability == Availability.UNKNOWN


def test_equal_lineup_markup_under_different_sides_is_not_a_duplicate():
    html = (
        '<table><tr><th>Side</th></tr><tr><td>'
        '<a href="/en/players/abcdef12/Player">Player</a></td></tr></table>'
    )
    page = parse_page_document(
        f'<div id="a" class="lineup">{html}</div><div id="b" class="lineup">{html}</div>',
        target_id="match", page_kind="match",
    )
    assert [(t.dataset, t.availability) for t in page.tables] == [
        ("table:lineup_a", Availability.AVAILABLE), ("table:lineup_b", Availability.AVAILABLE),
    ]
    assert page.tables[0].rows[0].row_id != page.tables[1].rows[0].row_id


def test_match_containers_with_unrecognised_material_rows_stay_unknown():
    table = "<table><tr><th>Notice</th></tr><tr><td>Generic text</td></tr></table>"
    html = f'<div id="a" class="lineup">{table}</div><div id="team_stats">{table}</div>'
    page = parse_page_document(html, target_id="match", page_kind="match")
    assert [t.dataset for t in page.tables] == ["table:lineup_a", "table:team_stats"]
    assert all(t.availability == Availability.UNKNOWN for t in page.tables)


@pytest.mark.parametrize(
    ("kind", "name_cell", "expected"),
    [
        ("competition_index", '<a href="/en/comps/9/history/Premier-League-Seasons">PL</a>', Availability.AVAILABLE),
        ("competition_index", "Unlinked name", Availability.UNKNOWN),
        ("competition_index", '<a href="https://example.org/en/comps/9/history/External">PL</a>', Availability.UNKNOWN),
        ("squad", '<a href="/en/comps/9/history/Premier-League-Seasons">PL</a>', Availability.UNKNOWN),
    ],
)
def test_index_section_requires_linked_competitions_and_source_fields(kind, name_cell, expected):
    html = (
        '<table id="comps_club"><tbody><tr><th data-stat="league_name">' + name_cell
        + '</th><td data-stat="gender">M</td><td data-stat="minseason">1992</td>'
        + '<td data-stat="maxseason">2026</td></tr></tbody></table>'
    )
    assert parse_page_document(html, target_id="t", page_kind=kind).tables[0].availability == expected


def test_schema_signature_does_not_change_with_repeated_rows():
    one = parse_page_document(
        """
        <table id="stats"><tr><th data-stat="player">Player</th></tr>
        <tr><td data-stat="player">A</td></tr></table>
        """,
        target_id="t",
        page_kind="season",
    )
    two = parse_page_document(
        """
        <table id="stats"><tr><th data-stat="player">Player</th></tr>
        <tr><td data-stat="player">A</td></tr>
        <tr><td data-stat="player">B</td></tr></table>
        """,
        target_id="t",
        page_kind="season",
    )

    assert one.tables[0].schema_signature == two.tables[0].schema_signature


def test_generic_records_keep_raw_header_value_and_entity_json():
    page = parse_page_document(
        """
        <table id="stats_test"><tr><th data-stat="player">Player</th></tr>
        <tr><td data-stat="player"><a href="/en/players/1234abcd/P">P</a></td></tr></table>
        """,
        target_id="t",
        page_kind="profile",
    )

    cells = page.cell_records()
    assert len(cells) == 1
    assert cells[0]["data_stat"] == "player"
    assert cells[0]["raw_value"] == "P"
    assert cells[0]["raw_header_path"] == '["Player"]'
    assert cells[0]["entity_ids"] == '{"player_id": "1234abcd"}'


def test_tfoot_is_preserved_as_total_without_changing_body_identity():
    table = """
    <table id="stats_standard">
      <thead><tr><th data-stat="squad">Squad</th></tr></thead>
      <tbody><tr><td data-stat="squad">Alpha</td></tr></tbody>
      %s
    </table>
    """
    body_only = parse_page_document(
        table % "", target_id="t", page_kind="squad", content_hash="fixed"
    )
    with_total = parse_page_document(
        table % """
        <tfoot><tr><th data-stat="squad">Squad Total</th></tr></tfoot>
        """,
        target_id="t",
        page_kind="squad",
        content_hash="fixed",
    )

    body_table = body_only.tables[0]
    total_table = with_total.tables[0]
    assert total_table.table_instance_id == body_table.table_instance_id
    assert total_table.rows[0].row_id == body_table.rows[0].row_id
    assert [row.cells[0].raw_value for row in total_table.rows] == [
        "Alpha", "Squad Total"
    ]
    assert [getattr(row, "is_total", None) for row in total_table.rows] == [
        False, True
    ]
    assert [cell.get("is_total") for cell in with_total.cell_records()] == [
        False, True
    ]


def test_tfoot_before_body_has_distinct_identity_without_moving_body_id():
    row = (
        '<th data-stat="squad">'
        '<a href="/en/squads/11111111/Alpha">Alpha</a></th>'
    )
    table = (
        '<table id="stats_standard"><thead><tr><th>Squad</th></tr></thead>'
        '{}</table>'
    )
    body_only = parse_page_document(
        table.format(f"<tbody><tr>{row}</tr></tbody>"),
        target_id="t",
        page_kind="squad",
        content_hash="fixed",
    )
    with_total = parse_page_document(
        table.format(
            f"<tfoot><tr>{row}</tr></tfoot><tbody><tr>{row}</tr></tbody>"
        ),
        target_id="t",
        page_kind="squad",
        content_hash="fixed",
    )

    total_row, body_row = with_total.tables[0].rows
    assert total_row.is_total is True
    assert body_row.is_total is False
    assert body_row.row_id == body_only.tables[0].rows[0].row_id
    assert total_row.row_id != body_row.row_id


def test_table_without_tbody_reads_direct_and_tfoot_rows_once():
    page = parse_page_document(
        """
        <table id="stats_standard">
          <tr><th data-stat="squad">Squad</th></tr>
          <tr><td data-stat="squad">Alpha</td></tr>
          <tfoot><tr><th data-stat="squad">Squad Total</th></tr></tfoot>
        </table>
        """,
        target_id="t",
        page_kind="squad",
    )

    rows = page.tables[0].rows
    assert [row.cells[0].raw_value for row in rows] == [
        "Alpha", "Squad Total"
    ]
    assert [getattr(row, "is_total", None) for row in rows] == [False, True]
    assert rows[0].cells[0].raw_header_path == ("Squad",)


def test_tfoot_th_row_is_not_promoted_to_fallback_header():
    page = parse_page_document(
        """
        <table id="stats_standard">
          <tfoot><tr><th data-stat="squad">Squad Total</th></tr></tfoot>
          <tr><th data-stat="squad">Squad</th></tr>
          <tr><td data-stat="squad">Alpha</td></tr>
        </table>
        """,
        target_id="t",
        page_kind="squad",
    )

    rows = page.tables[0].rows
    assert [row.cells[0].raw_value for row in rows] == [
        "Squad Total", "Alpha"
    ]
    assert [row.is_total for row in rows] == [True, False]
    assert all(row.cells[0].raw_header_path == ("Squad",) for row in rows)


def test_empty_tbody_does_not_trigger_table_fallback():
    page = parse_page_document(
        """
        <table id="stats_standard">
          <thead><tr><th data-stat="squad">Squad</th></tr></thead>
          <tr><td data-stat="squad">Outside body</td></tr>
          <tbody></tbody>
          <tfoot><tr><th data-stat="squad">Squad Total</th></tr></tfoot>
        </table>
        """,
        target_id="t",
        page_kind="squad",
    )

    rows = page.tables[0].rows
    assert [row.cells[0].raw_value for row in rows] == ["Squad Total"]
    assert [getattr(row, "is_total", None) for row in rows] == [True]


def test_incomplete_and_nested_rows_are_not_duplicated_across_sections():
    page = parse_page_document(
        """
        <table id="stats_standard">
          <thead><tr><th data-stat="squad">Squad</th></tr></thead>
          <tbody>
            <tr><td></td></tr>
            <tr class="thead"><th>Squad</th></tr>
            <tr><td data-stat="squad"><span>Alpha</span></td></tr>
          </tbody>
          <tfoot>
            <tr><td></td></tr>
            <tr><th data-stat="squad">Squad Total</th></tr>
          </tfoot>
        </table>
        """,
        target_id="t",
        page_kind="squad",
    )

    rows = page.tables[0].rows
    assert [row.cells[0].raw_value for row in rows] == [
        "Alpha", "Squad Total"
    ]
    assert [getattr(row, "is_total", None) for row in rows] == [False, True]


def test_a_broken_colspan_must_not_discard_the_whole_page():
    """FBref's player pages ship colspan="" on every wages row header, and one
    cell per page reaches the parser as colspan='class="' (an unquoted attribute
    the markup folds into the span). int() raised, the generic layer reported
    parser errors, and the entire player page was rejected — a lossless capture
    must never lose a page over a rendering hint."""
    html = """
    <!--
    <div class="table_container" id="div_wages">
      <table id="wages">
        <thead><tr>
          <th colspan="" data-stat="year">Year</th>
          <th colspan='class="' data-stat="team">Team</th>
          <th colspan="2" data-stat="wages">Wages</th>
        </tr></thead>
        <tbody><tr>
          <th colspan="" data-stat="year" scope="row">2023-2024</th>
          <td data-stat="team">Lyon</td>
          <td data-stat="weekly">100</td>
          <td data-stat="annual">5200</td>
        </tr></tbody>
      </table>
    </div>
    -->
    """

    page = parse_page_document(
        html, target_id="fbref:player:9dbb75ca", page_kind="player",
        content_hash="abc",
    )

    assert page.errors == ()
    wages = [table for table in page.tables if table.table_id == "wages"]
    assert len(wages) == 1
    assert wages[0].row_count == 1


def test_rows_repeating_the_same_entities_get_distinct_ids():
    """FBref repeats the same entities across rows of one table — a player's
    stats carry two rows for the same club. Entity identity alone is not a key:
    the colliding row_ids reached Iceberg as duplicate MERGE source rows
    (MERGE_TARGET_ROW_MULTIPLE_MATCHES) and the whole page failed to persist."""
    html = """
    <table id="stats_standard">
      <thead><tr><th data-stat="season">Season</th><th data-stat="team">Squad</th>
                 <th data-stat="goals">Gls</th></tr></thead>
      <tbody>
        <tr><th data-stat="season">2022-2023</th>
            <td data-stat="team"><a href="/en/squads/d53c0b06/Lyon-Stats">Lyon</a></td>
            <td data-stat="goals">18</td></tr>
        <tr><th data-stat="season">2023-2024</th>
            <td data-stat="team"><a href="/en/squads/d53c0b06/Lyon-Stats">Lyon</a></td>
            <td data-stat="goals">19</td></tr>
      </tbody>
    </table>
    """

    page = parse_page_document(
        html, target_id="fbref:player:9dbb75ca", page_kind="player",
        content_hash="abc",
    )
    table = page.tables[0]
    row_ids = [row.row_id for row in table.rows]
    cells = page.cell_records()
    merge_keys = {
        (cell["table_instance_id"], cell["row_id"], cell["cell_id"])
        for cell in cells
    }

    assert len(row_ids) == 2
    assert len(set(row_ids)) == 2
    assert len(merge_keys) == len(cells)

    soup = BeautifulSoup(html, "html.parser")
    first, second = soup.tbody.find_all("tr")
    first.insert_before(second.extract())
    second.find("td", attrs={"data-stat": "goals"}).string = "20"
    reordered = parse_page_document(str(soup), target_id=page.target_id, page_kind=page.page_kind)
    assert [r.row_id for r in reordered.tables[0].rows] == row_ids[::-1]


# Shape of the real 1938 Austria squad response: a full 200 page whose only
# content is a note about a withdrawn World Cup tie, head and all.
TABLELESS_SQUAD_ARCHIVE = """
<html><head>
  <link rel="canonical"
    href="https://fbref.com/en/squads/d5121f10/1938/Austria-Men-Stats"/>
  <meta property="og:url"
    content="https://fbref.com/en/squads/d5121f10/1938/Austria-Men-Stats">
  <meta property="og:type" content="    Organization" />
</head><body>
  <div id="meta"><h1><span>1938 Austria Stats</span></h1></div>
  <div id="content"><p>Round of 16: Sweden advance to Quarter-finals.</p></div>
</body></html>
"""

ARCHIVE_URL = "https://fbref.com/en/squads/d5121f10/1938/Austria-Men-Stats"


def test_the_sources_own_tableless_page_proves_it_owns_the_target_url():
    assert response_owns_target_page(
        TABLELESS_SQUAD_ARCHIVE, canonical_url=ARCHIVE_URL
    )
    # A trailing slash is the same address.
    assert response_owns_target_page(
        TABLELESS_SQUAD_ARCHIVE, canonical_url=ARCHIVE_URL + "/"
    )


@pytest.mark.parametrize(
    "mutation",
    [
        # A foreign 200 shell: same coarse shape, no evidence at all.
        lambda html: "<html><head></head><body>502</body></html>",
        # Advertises a different target: retiring ours would be wrong.
        lambda html: html.replace("/1938/", "/1954/"),
        # Only one of the two source-supplied addresses survives.
        lambda html: html.replace('<link rel="canonical"', "<link rel=alt"),
        lambda html: html.replace('property="og:url"', 'property="og:x"'),
        # No header block of the source's own template.
        lambda html: html.replace('<div id="meta">', '<div id="other">'),
        # Header block present but empty.
        lambda html: html.replace("<span>1938 Austria Stats</span>", ""),
        # Plain http is not the source's canonical scheme.
        lambda html: html.replace("https://fbref.com", "http://fbref.com"),
        # A look-alike host must not pass for fbref.com.
        lambda html: html.replace("fbref.com", "fbref.com.example.net"),
    ],
)
def test_a_tableless_response_without_full_evidence_is_not_the_target_page(
    mutation,
):
    assert not response_owns_target_page(
        mutation(TABLELESS_SQUAD_ARCHIVE), canonical_url=ARCHIVE_URL
    )


def test_target_without_a_canonical_url_can_never_prove_ownership():
    assert not response_owns_target_page(
        TABLELESS_SQUAD_ARCHIVE, canonical_url=""
    )


SEASON_STATS_URL = (
    "https://fbref.com/en/comps/657/keepers/"
    "Africa-Cup-of-Nations-qualification-Stats"
)
# Shape of the real 657:2027:keepers capture (content_hash e9cfdc24…): a full
# 200 response for an edition whose stat route has no rows yet.
TABLELESS_SEASON_STATS = """
<html><head>
  <link rel="canonical"
    href="https://fbref.com/en/comps/657/keepers/Africa-Cup-of-Nations-qualification-Stats" />
  <meta property="og:url"
    content="https://fbref.com/en/comps/657/keepers/Africa-Cup-of-Nations-qualification-Stats">
</head><body>
  <div id="meta"><h1>2027 Africa Cup of Nations qualification Goalkeeper Stats</h1></div>
</body></html>
"""


def test_tableless_season_stats_page_with_proven_identity_is_valid():
    page = parse_page_document(
        TABLELESS_SEASON_STATS,
        target_id="fbref:season_stats:657:2027:keepers",
        page_kind="season_stats",
        source_ids={
            "competition_id": "657",
            "season_id": "2027",
            "stat_route": "keepers",
        },
        canonical_url=SEASON_STATS_URL,
    )

    assert page.errors == ()
    assert page.tables == ()


def test_tableless_season_stats_shell_without_identity_is_rejected():
    shell = "<html><body><p>temporary source shell</p></body></html>"

    page = parse_page_document(
        shell,
        target_id="fbref:season_stats:657:2027:keepers",
        page_kind="season_stats",
        source_ids={"competition_id": "657", "season_id": "2027"},
        canonical_url=SEASON_STATS_URL,
    )

    assert page.errors == ("page_contract:no_tables",)


def test_tableless_season_stats_without_a_canonical_url_is_rejected():
    # A caller that cannot name the target's own address has no evidence to
    # tell this page from a 200 shell.
    page = parse_page_document(
        TABLELESS_SEASON_STATS,
        target_id="fbref:season_stats:657:2027:keepers",
        page_kind="season_stats",
        source_ids={"competition_id": "657", "season_id": "2027"},
    )

    assert page.errors == ("page_contract:no_tables",)


def test_tableless_season_stats_of_another_target_is_rejected():
    page = parse_page_document(
        TABLELESS_SEASON_STATS,
        target_id="fbref:season_stats:657:2027:misc",
        page_kind="season_stats",
        source_ids={"competition_id": "657", "season_id": "2027"},
        canonical_url=(
            "https://fbref.com/en/comps/657/misc/"
            "Africa-Cup-of-Nations-qualification-Stats"
        ),
    )

    assert page.errors == ("page_contract:no_tables",)
