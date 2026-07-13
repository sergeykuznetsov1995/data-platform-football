from scrapers.fbref.page_document import Availability, parse_page_document


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

    assert first.tables[0].table_id.startswith("anon_0_")
    assert first.tables[0].table_id == second.tables[0].table_id
    assert first.tables[0].table_instance_id == second.tables[0].table_instance_id
    assert first.tables[0].rows[0].row_id == second.tables[0].rows[0].row_id


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
