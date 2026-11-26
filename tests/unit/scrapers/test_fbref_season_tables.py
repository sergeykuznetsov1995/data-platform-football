import gzip
import hashlib
from pathlib import Path

import pandas as pd
import pytest

from scrapers.base.trino_manager import TrinoTableManager
from scrapers.fbref.match_parser import DatasetStatus
from scrapers.fbref.season_tables import parse_squad_roster_html, parse_standings_html
from scrapers.fbref.typed_bronze import (
    FBrefTypedBronzeWriter,
    TypedBronzePersistenceError,
    TypedSourceContext,
    parse_season_stats_html,
)


ATLAS = (
    Path(__file__).resolve().parents[2]
    / "fixtures/fbref/squads/atlas-2009-2010-7a795569.html.gz"
)
SQUAD_ID = "7c76bc53"
URL = f"https://fbref.com/en/squads/{SQUAD_ID}/2009-2010/c31/Atlas-Stats-Liga-MX"
TARGET = "fbref:squad:7c76bc53:a667b6f50d092c9f077c"
INDIA = (
    Path(__file__).resolve().parents[2]
    / "fixtures/fbref/season_stats/indian-super-league-2026-2027-e260cb62.html.gz"
)


def test_real_source_roster_and_repeated_standings_tables():
    html = gzip.decompress(ATLAS.read_bytes())
    roster, season = parse_squad_roster_html(html, canonical_url=URL, squad_id=SQUAD_ID)
    assert season == "2009-2010"
    assert roster.status == DatasetStatus.AVAILABLE
    assert roster.row_count == 34
    assert roster.frame.squad_id.unique().tolist() == [SQUAD_ID]
    assert roster.frame.source_competition_id.unique().tolist() == ["31"]
    assert roster.frame.loc[
        0, ["player_id", "player", "position", "minutes"]
    ].tolist() == [
        "aad4f74f",
        "Mariano Barbosa",
        "GK",
        2514,
    ]
    assert not roster.frame.player.str.contains("Total").any()

    # The archive also contains six distinct overall groups (and a home/away
    # table). Two pairs of groups share their native table ID.
    groups = parse_standings_html(html)
    assert groups.row_count == 45
    assert (
        len(groups.frame[["source_table_id", "source_table_caption"]].drop_duplicates())
        == 6
    )
    assert groups.frame.loc[0, ["team_id", "team", "rank", "points"]].tolist() == [
        "2ae45a0c",
        "San Luis",
        2,
        21,
    ]
    assert str(groups.frame.xg_for.dtype) == "Float64"


STANDING = (
    '<table id="results2026_overall"><thead><tr><th data-stat="team">Team</th></tr></thead>'
    '<tbody><tr><td data-stat="team"><a href="/en/squads/1234abcd/Union">Union</a></td>'
    '<td data-stat="rank">1</td></tr></tbody></table>'
)


def test_season_overview_includes_commented_standings_once_and_stat_route_does_not():
    html = STANDING + "<!-- " + STANDING + " -->"
    context = TypedSourceContext("9", "2026")
    overview = parse_season_stats_html(
        html, context=context, stat_route="standard", include_standings=True
    )
    assert overview["standings"].row_count == 1
    assert overview["standings"].frame.team_id.tolist() == ["1234abcd"]
    assert "standings" not in parse_season_stats_html(
        html, context=context, stat_route="standard"
    )


def test_archived_india_overview_has_native_competition_and_team_ids():
    html = gzip.decompress(INDIA.read_bytes())
    assert (
        hashlib.sha256(html).hexdigest()
        == "e260cb62ab62441227d5a62c72d62cbf90867ae530cf42fa20efbec435c0ed4f"
    )
    parsed = parse_season_stats_html(
        html,
        context=TypedSourceContext("82", "2026-2027"),
        stat_route="standard",
        include_standings=True,
        canonical_url="https://fbref.com/en/comps/82/Indian-Super-League-Stats",
    )["standings"]
    assert parsed.row_count == 13
    assert parsed.frame.source_table_id.unique().tolist() == [
        "results2026-2027821_overall"
    ]
    assert parsed.frame.team_id.nunique() == 13
    assert parsed.frame.loc[0, ["team", "team_id", "points"]].tolist() == [
        "Chennaiyin",
        "6f0be699",
        0,
    ]


@pytest.mark.parametrize(
    "markup,status",
    [
        ('<table id="results1_home_away"></table>', DatasetStatus.NOT_APPLICABLE),
        (
            '<table id="results1_overall"><thead><tr><th data-stat="team">Squad</th></tr></thead><tbody></tbody></table>',
            DatasetStatus.EMPTY,
        ),
        (
            '<table id="results1_overall"><tr><td data-stat="points">3</td></tr></table>',
            DatasetStatus.ERROR,
        ),
        ('<table id="results1_overall"></table>', DatasetStatus.ERROR),
        (
            STANDING
            + "<!-- "
            + STANDING.replace('data-stat="rank">1', 'data-stat="rank">2')
            + " -->",
            DatasetStatus.ERROR,
        ),
    ],
)
def test_standings_distinguish_absent_empty_and_unparsed(markup, status):
    assert parse_standings_html(markup).status == status


def _roster(names, season="2026", table="stats_standard_combined"):
    rows = "".join(
        f'<tr><th scope="row" data-stat="player"><a href="/en/players/{native}/x">{name}</a></th></tr>'
        for native, name in names
    )
    return (f"<h1>{season} Atlas Stats</h1>" if season else "") + (
        f'<table id="{table}"><thead><tr><th data-stat="player">Player</th></tr></thead>'
        f"<tbody>{rows}</tbody></table>"
    )


def test_shared_squad_target_has_its_own_edition_and_no_invented_competition():
    url = f"https://fbref.com/en/squads/{SQUAD_ID}/Atlas-Stats"
    html = _roster([("1234abcd", "One")])
    parsed, season = parse_squad_roster_html(html, canonical_url=url, squad_id=SQUAD_ID)
    assert season == "2026" and pd.isna(parsed.frame.at[0, "source_competition_id"])
    _, undated = parse_squad_roster_html(
        _roster([], season=None), canonical_url=url, squad_id=SQUAD_ID
    )
    assert undated is None
    bad, _ = parse_squad_roster_html(html, canonical_url=URL, squad_id=SQUAD_ID)
    assert bad.status == DatasetStatus.ERROR
    mismatch, _ = parse_squad_roster_html(html, canonical_url=url, squad_id="abcd1234")
    assert mismatch.status == DatasetStatus.ERROR


def test_explicit_roster_in_comments_takes_precedence_over_appearance_statistics():
    html = (
        _roster([("1234abcd", "One")])
        + "<!-- "
        + _roster([("abcd1234", "Two")], table="roster")
        + " -->"
    )
    parsed, _ = parse_squad_roster_html(
        html,
        canonical_url=f"https://fbref.com/en/squads/{SQUAD_ID}/Atlas-Stats",
        squad_id=SQUAD_ID,
    )
    assert parsed.frame.player.tolist() == ["Two"]


def test_roster_preserves_a_player_with_only_a_header_cell_and_combined_competitions():
    html = _roster([("1234abcd", "One")], season="2009-2010").replace(
        'scope="row" ', ""
    )
    parsed, _ = parse_squad_roster_html(html, canonical_url=URL, squad_id=SQUAD_ID)
    assert parsed.frame.player_id.tolist() == ["1234abcd"]
    assert parsed.frame.source_competition_id.isna().all()
    missing = '<table id="roster"><thead><tr><th data-stat="squad">Squad</th></tr></thead></table>'
    malformed, _ = parse_squad_roster_html(
        missing, canonical_url=URL, squad_id=SQUAD_ID
    )
    assert malformed.status == DatasetStatus.ERROR


def test_native_ids_are_never_invented_and_contradictory_ids_fail():
    text_row = '<tr><th data-stat="player">New signing</th></tr>'
    parsed, _ = parse_squad_roster_html(
        f'<table id="roster">{text_row}</table>', canonical_url=URL, squad_id=SQUAD_ID
    )
    assert parsed.frame.player.tolist() == ["New signing"]
    assert parsed.frame.player_id.isna().all()
    conflicting = STANDING.replace(
        'data-stat="team"><a', 'data-stat="team" data-append-csv="ffffffff"><a'
    )
    assert parse_standings_html(conflicting).status == DatasetStatus.ERROR


def _roster_with_season_cells(*players):
    rows = "".join(
        f'<tr><th data-stat="player">{name}</th><td data-stat="birth_year">1995</td>{cells}</tr>'
        for name, cells in players
    )
    return f"""
        <nav><h1>1990 Atlas</h1><a href="/en/squads/{SQUAD_ID}/1990/archive">1990</a>
          <!-- <table id="roster"><tr><td data-stat="player">Navigation</td>
               <td data-stat="season">1990</td></tr></table> -->
        </nav>
        <main><table id="history"><tr><td data-stat="year_id">2000</td></tr></table>
          <table id="stats_standard_31"><tr><td data-stat="player">Unused</td>
            <td data-stat="season">2010</td></tr></table>
          <!-- <table id="roster"><tbody>{rows}</tbody><tfoot><tr>
               <td data-stat="year_id">2004</td></tr></tfoot></table> -->
        </main>
    """


@pytest.mark.parametrize("field", ["season", "year_id"])
def test_roster_uses_only_the_selected_tables_explicit_season(field):
    url = f"https://fbref.com/en/squads/{SQUAD_ID}/Atlas-Stats"
    cell = f'<td data-stat="{field}">2024–2025</td>'
    parsed, season = parse_squad_roster_html(
        _roster_with_season_cells(("One", cell), ("Two", cell)),
        canonical_url=url,
        squad_id=SQUAD_ID,
    )
    assert season == "2024-2025"
    assert parsed.frame.source_season_id.tolist() == [season, season]
    assert parsed.frame.player.tolist() == ["One", "Two"]
    no_evidence = '<td data-stat="year_id">—</td><td><a href="/en/matches/2024/game">2024</a></td>'
    unknown, season = parse_squad_roster_html(
        _roster_with_season_cells(("One", no_evidence)),
        canonical_url=url,
        squad_id=SQUAD_ID,
    )
    assert season is None and unknown.frame.source_season_id.isna().all()


@pytest.mark.parametrize(
    ("page_heading", "other_row", "column", "expected"),
    [
        ("", "2025", "2024", DatasetStatus.ERROR),
        ("<h1>2025 Atlas</h1>", "2024", "2024", DatasetStatus.ERROR),
        ("", "", "2024", DatasetStatus.ERROR),
        ("<h1>2024 Atlas</h1>", "", "2024", DatasetStatus.AVAILABLE),
        ("", "unknown edition", "2024", DatasetStatus.ERROR),
        ("", "2024", '2024</td><td data-stat="year_id">2023', DatasetStatus.ERROR),
    ],
)
def test_roster_rejects_ambiguous_snapshot_editions(
    page_heading, other_row, column, expected
):
    markup = _roster_with_season_cells(
        ("One", f'<td data-stat="season">{column}</td>'),
        ("Two", f'<td data-stat="year_id">{other_row}</td>'),
    )
    result, _ = parse_squad_roster_html(
        page_heading + markup,
        canonical_url=f"https://fbref.com/en/squads/{SQUAD_ID}/Atlas-Stats",
        squad_id=SQUAD_ID,
    )
    assert result.status == expected


@pytest.mark.parametrize(
    "advertised",
    [
        "https://fbref.com/en/comps/10/2026/standings/Standings",
        "https://fbref.com/en/comps/9/2025/standings/Standings",
    ],
)
def test_standings_from_another_competition_or_edition_do_not_clear_target(advertised):
    parsed = parse_standings_html(
        f'<link rel="canonical" href="{advertised}">' + STANDING,
        canonical_url="https://fbref.com/en/comps/9/2026/standings/Standings",
        competition_id="9",
        season_id="2026",
    )
    assert parsed.status == DatasetStatus.ERROR


class LocalTables(TrinoTableManager):
    """Execute the writer's actual MERGE with SQL tables, including an ambiguous-commit replay."""

    def __init__(self):
        super().__init__(catalog="memory")
        self.db = pytest.importorskip("duckdb").connect()
        self.db.execute("CREATE SCHEMA bronze")
        self.short_stage = False
        self.merges = []

    def table_exists(self, schema, table):
        return bool(
            self.db.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name=?",
                [schema, table],
            ).fetchall()
        )

    def create_iceberg_table(self, schema, table, columns, partition_columns=None):
        fields = ", ".join(f'"{name}" {kind}' for name, kind in columns.items())
        self.db.execute(f"CREATE TABLE {schema}.{table} ({fields})")

    def get_table_columns(self, schema, table):
        return {
            row[0]: row[1]
            for row in self.db.execute(f"DESCRIBE {schema}.{table}").fetchall()
        }

    def insert_dataframe(self, schema, table, df, *, column_types):
        frame = df.iloc[:0] if self.short_stage else df
        self.db.register("candidate", frame)
        self.db.execute(f"INSERT INTO {schema}.{table} SELECT * FROM candidate")
        return len(frame)

    def execute_query(self, sql):
        return self.db.execute(sql).fetchall()

    def _execute_replacement(self, sql):
        self.db.execute(sql)
        self.db.execute(sql)
        self.merges.append(sql)

    def drop_table(self, schema, table, if_exists=True):
        self.db.execute(f"DROP TABLE {schema}.{table}")

    def insert_dataframe_atomic(
        self, schema, table, df, *, delete_filter, single_statement_replace
    ):
        assert df.empty and single_statement_replace
        self.db.execute(f"DELETE FROM {schema}.{table} WHERE {delete_filter}")
        return 0


def test_scope_replacement_is_idempotent_with_nulls_and_does_not_delete_another_edition_or_target():
    manager = LocalTables()
    writer = FBrefTypedBronzeWriter(manager)
    url = f"https://fbref.com/en/squads/{SQUAD_ID}/Atlas-Stats"

    def save(html, target=TARGET):
        parsed, season = parse_squad_roster_html(
            html, squad_id=SQUAD_ID, canonical_url=url
        )
        return writer.persist_table_snapshot(
            parsed,
            context=None,
            source_target_id=target,
            season_id=season,
            run_id="run",
        )

    def players():
        return manager.execute_query(
            "SELECT source_target_id, source_season_id, player FROM bronze.fbref_squad_roster "
            "ORDER BY source_target_id,source_season_id,player"
        )

    a = ("1234abcd", "One")
    b = ("abcd1234", "Two")
    save(_roster([a], season="2025"))
    save(_roster([b]), target="another-squad-page")
    save(_roster([a, b]))
    assert save(_roster([b])) == {"squad_roster": 1}
    expected = [
        ("another-squad-page", "2026", "Two"),
        (TARGET, "2025", "One"),
        (TARGET, "2026", "Two"),
    ]
    assert players() == expected
    assert save("<html><body><p>Roster not published</p></body></html>") == {}
    assert players() == expected
    manager.short_stage = True
    with pytest.raises(TypedBronzePersistenceError, match="Incomplete stage"):
        save(_roster([a]))
    assert players() == expected
    assert save(_roster([])) == {"squad_roster": 0}
    assert players() == expected[:2]
    manager.short_stage = False
    save(_roster([a], season=None))
    save(_roster([b], season=None))
    assert (TARGET, None, "Two") in players()
    save(_roster([], season=None))
    assert players() == expected[:2]


def test_standings_replace_only_the_original_overview_or_standalone_target():
    manager = LocalTables()
    writer = FBrefTypedBronzeWriter(manager)
    context = TypedSourceContext("9", "2026")
    overview, standalone = "fbref:season:9:2026", "fbref:standings:9:2026"

    def save(html, target):
        return writer.persist_season_stats(
            {"standings": parse_standings_html(html)},
            context=context,
            run_id="run",
            target_identity="refresh",
            source_target_id=target,
        )

    def ranks():
        return manager.execute_query(
            "SELECT source_target_id, source_competition_id, source_season_id, team_id, rank "
            "FROM bronze.fbref_standings ORDER BY source_target_id"
        )

    save(STANDING, overview)
    save(STANDING, standalone)
    assert save(
        STANDING.replace('data-stat="rank">1', 'data-stat="rank">2'), overview
    ) == {"standings": 1}
    expected = [
        (overview, "9", "2026", "1234abcd", 2),
        (standalone, "9", "2026", "1234abcd", 1),
    ]
    assert ranks() == expected
    with pytest.raises(TypedBronzePersistenceError, match="parser errors"):
        save('<table id="results8_overall"></table>', overview)
    assert ranks() == expected
    save(
        '<table id="results8_overall"><thead><tr><th data-stat="team">Squad</th></tr></thead></table>',
        overview,
    )
    assert ranks() == expected[1:]


def test_table_only_roster_edition_replays_the_same_target_and_preserves_its_other_edition():
    manager = LocalTables()
    writer = FBrefTypedBronzeWriter(manager)
    url = f"https://fbref.com/en/squads/{SQUAD_ID}/Atlas-Stats"

    def save(edition, players):
        html = _roster_with_season_cells(
            *((name, f'<td data-stat="year_id">{edition}</td>') for name in players)
        )
        parsed, season = parse_squad_roster_html(
            html, canonical_url=url, squad_id=SQUAD_ID
        )
        return writer.persist_table_snapshot(
            parsed,
            source_target_id=TARGET,
            season_id=season,
            context=None,
            run_id="local-replay",
        )

    save("2024", ["Former"])
    save("2025", ["One", "Two"])
    save("2025", ["Two"])
    assert save("2025", ["Two"]) == {"squad_roster": 1}
    assert manager.execute_query(
        "SELECT source_target_id, source_season_id, season, player FROM bronze.fbref_squad_roster "
        "ORDER BY source_season_id, player"
    ) == [(TARGET, "2024", 2024, "Former"), (TARGET, "2025", 2025, "Two")]
