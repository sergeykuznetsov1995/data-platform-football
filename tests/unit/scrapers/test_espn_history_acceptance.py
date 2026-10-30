"""Offline tier A evidence, acceptance and parity queries on DuckDB."""
from __future__ import annotations

import json

import pytest

from scrapers.espn import history
from scrapers.espn.history_acceptance import (
    CoreEvidence, core_evidence, render_acceptance, render_acceptance_sql,
    render_parity, render_parity_sql, render_legacy_parts_sql,
)
from scrapers.espn.parser_common import EspnParseError
from tests.unit.scrapers.test_espn_history_report import db  # noqa: F401

pytestmark = pytest.mark.unit


def test_acceptance_left_joins_scope_and_counts_red_pairs_once(db):
    scope = (("eng.1", 2016), ("eng.1", 2017), ("eng.1", 2018), ("ger.1", 2016))
    history.save_rows(db.con, [
        history.QueueRow("eng.1", 2016, 1, history.DONE),
        history.QueueRow("eng.1", 2016, 2, history.DONE),
        history.QueueRow("eng.1", 2017, 1, history.RED),
        history.QueueRow("eng.1", 0, 0, history.RED),
        history.QueueRow("ger.1", 2016, 1, history.LISTED),
        history.QueueRow("other", 2001, 1, history.RED),
    ])
    for event in (1, 2, 3):
        db.match(event, season_year=2016, first_published_at="2026-01-01")
    evidence = {("eng.1", 2016): CoreEvidence(2016, ((1, (1, 2)), (2, (2, 3))))}
    rows = db.run(render_acceptance_sql(scope, evidence, {pair: True for pair in scope}))
    assert len(rows) == 4  # unrelated red queue is absent
    assert [r[4] for r in rows] == [False, True, True, False]  # inventory affects only absent pairs
    assert rows[0][6:12] == (4, 3, 1, 3, 0, 0)  # raw count, union, cross-type duplicate
    assert rows[0][12] is True
    assert rows[2][2] is True  # missing pair remains in denominator
    assert rows[3][3] is True  # listed blocks acceptance
    assert "приёмка: нет" in render_acceptance(rows)


@pytest.mark.parametrize("red,accepted", [(19, True), (20, False)])
def test_strict_red_share_boundary(db, red, accepted):
    scope = tuple(("eng.1", 1900 + i) for i in range(100))
    history.save_rows(db.con, [history.QueueRow(slug, year, 0,
                      history.RED if i < red else history.EMPTY, attempts=history.MAX_ATTEMPTS)
                      for i, (slug, year) in enumerate(scope)])
    evidence = {pair: CoreEvidence(pair[1], ()) for pair in scope[red:]}
    rows = db.run(render_acceptance_sql(scope, evidence, {pair: True for pair in scope}))
    assert sum(r[4] for r in rows) == red
    assert ("приёмка: да" in render_acceptance(rows)) is accepted


@pytest.mark.parametrize("state", [history.PENDING, history.LISTED])
def test_unfinished_pair_blocks_even_below_red_threshold(db, state):
    history.save_rows(db.con, [history.QueueRow("eng.1", 2016, 1, state)])
    rows = db.run(render_acceptance_sql((("eng.1", 2016),), {
        ("eng.1", 2016): CoreEvidence(2016, ((1, ()),)),
    }, {("eng.1", 2016): True}))
    assert not rows[0][12]
    assert "приёмка: нет" in render_acceptance(rows)


def test_missing_evidence_and_missing_type_block_completion(db):
    scope = (("eng.1", 2016),)
    history.save_rows(db.con, [history.QueueRow("eng.1", 2016, 1, history.DONE)])
    rows = db.run(render_acceptance_sql(scope, {}, {pair: True for pair in scope}))
    assert "приёмка: нет" in render_acceptance(rows)
    rows = db.run(render_acceptance_sql(scope, {
        scope[0]: CoreEvidence(2016, ((1, ()), (2, ()))),
    }, {pair: True for pair in scope}))
    assert not rows[0][12]


def test_completeness_compares_ids_not_queue_matches_or_just_counts(db):
    scope = (("eng.1", 2016),)
    history.save_rows(db.con, [history.QueueRow("eng.1", 2016, 1, history.DONE,
                                               matches=2, done=2)])
    db.match(1, season_year=2016, first_published_at="2026-01-01")
    db.match(2, season_year=2016)  # not published
    db.match(3, season_year=2016, first_published_at="2026-01-01")  # unexpected id
    rows = db.run(render_acceptance_sql(
        scope, {scope[0]: CoreEvidence(2016, ((1, (1, 2)),))}, {pair: True for pair in scope}))
    assert rows[0][4] is True
    assert rows[0][10:12] == (1, 1)
    assert rows[0][6] == rows[0][9] == 2  # equal counts do not prove completeness


def test_confirmed_quality_defect_is_red(db):
    scope = (("eng.1", 2016),)
    history.save_rows(db.con, [history.QueueRow("eng.1", 2016, 0, history.EMPTY)])
    rows = db.run(render_acceptance_sql(
        scope, {scope[0]: CoreEvidence(2016, ())}, {pair: False for pair in scope}))
    assert rows[0][4] is True


def test_core_evidence_validates_counts_pages_and_all_types():
    from tests.unit.scrapers.test_espn_history import PROBES, ENG_2015_PAGES

    season = json.loads((PROBES / "season_eng1_2015.json").read_bytes())
    pages = {1: [p.read_bytes() for p in ENG_2015_PAGES]}
    evidence = core_evidence(season, pages)
    assert evidence.year == 2015
    assert sum(len(ids) for _, ids in evidence.types) == 380
    with pytest.raises(EspnParseError, match="exactly all"):
        core_evidence(season, {})
    with pytest.raises(EspnParseError, match="lacks a page"):
        core_evidence(season, {1: pages[1][:-1]})
    broken = json.loads(pages[1][0])
    broken["count"] += 1
    with pytest.raises(EspnParseError, match="count"):
        core_evidence(season, {1: [json.dumps(broken).encode(), *pages[1][1:]]})


def test_explicit_unavailable_exclusions_need_reason_and_cannot_open_all(tmp_path):
    path = tmp_path / "scope.json"
    payload = {"allow": [["eng.1", 2016], ["eng.1", 2017]],
               "excluded": [["eng.1", 2016, "confirmed unavailable: owner decision"]]}
    path.write_text(json.dumps(payload))
    assert history.load_scope(path) == (("eng.1", 2017),)
    payload["excluded"][0][2] = ""
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError):
        history.load_scope(path)
    payload["excluded"] = [["eng.1", year, "unavailable"] for year in (2016, 2017)]
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="cannot empty"):
        history.load_scope(path)


@pytest.fixture()
def legacy(db):
    db.con.execute("ALTER TABLE iceberg.bronze.espn_match ADD COLUMN home_score integer")
    db.con.execute("ALTER TABLE iceberg.bronze.espn_match ADD COLUMN away_score integer")
    db.con.execute("CREATE TABLE iceberg.bronze.espn_schedule "
                   "(league varchar, season varchar, game_id varchar, match_date varchar, "
                   "home_goals varchar, away_goals varchar, game varchar)")
    for table in ("espn_lineup", "espn_matchsheet"):
        db.con.execute(f"CREATE TABLE iceberg.bronze.{table} (league varchar, season varchar, game varchar)")
    return db


def test_parity_matches_ids_across_wrong_season_and_reports_null_scores(legacy):
    db = legacy
    db.con.execute("""INSERT INTO iceberg.bronze.espn_match
        (competition_slug, season_year, event_id, kickoff, home_score, away_score, first_published_at)
        VALUES ('eng.1',2016,1,'2016-08-20',2,1,'2026-01-01'),
               ('eng.1',2016,2,'2016-08-21',0,0,'2026-01-01'),
               ('eng.1',2016,4,'2016-08-22',1,0,'2026-01-01'),
               ('ita.1',2020,10,'2020-08-20',1,0,'2026-01-01')""")
    db.con.execute("""INSERT INTO iceberg.bronze.espn_schedule VALUES
        ('ENG-Premier League','1516','1','2016-08-20','2','1','a'),
        ('ENG-Premier League','1617','2','2016-08-23',NULL,NULL,'b'),
        ('ENG-Premier League','1617','3','2016-08-23','1','0','c'),
        ('ITA-Serie A','2021','10','2019-08-20','1','0','d')""")
    rows = db.run(render_parity_sql((("eng.1", 2016), ("ita.1", 2020))))
    by_id = {r[3]: r for r in rows}
    assert by_id[1][9:12] == (False, False, True)  # shifted label, correct date/score
    assert by_id[2][9:12] == (True, None, False)  # NULL score is uncomparable
    assert by_id[3][5] is True  # legacy-only
    assert by_id[4][4] is True  # native-only
    assert by_id[10][4] is True  # ITA '2021' is excluded, not a score mismatch
    assert "season_mismatch=True" in render_parity(rows)


def test_parity_keeps_ambiguous_links_and_parts_uncomparable(legacy):
    db = legacy
    db.con.execute("""INSERT INTO iceberg.bronze.espn_match
        (competition_slug,season_year,event_id,first_published_at)
        VALUES ('eng.1',2016,1,'2026-01-01'),('eng.1',2016,1,'2026-01-01')""")
    db.con.execute("""INSERT INTO iceberg.bronze.espn_schedule VALUES
        ('ENG-Premier League','1617','1',NULL,NULL,NULL,'a'),
        ('ENG-Premier League','1617','1',NULL,NULL,NULL,'a'),
        ('ENG-Premier League','1516','1',NULL,NULL,NULL,'x')""")
    db.con.execute("INSERT INTO iceberg.bronze.espn_lineup VALUES "
                   "('ENG-Premier League','1617','a'),('ENG-Premier League','1617','missing')")
    db.con.execute("INSERT INTO iceberg.bronze.espn_matchsheet VALUES ('ENG-Premier League','1617',NULL)")
    rows = db.run(render_parity_sql((("eng.1", 2016),)))
    assert len(rows) == 2
    assert all(r[8] and r[9] is None and r[10] is None for r in rows)
    parts = db.run(render_legacy_parts_sql((("eng.1", 2016),)))
    assert ("lineup", "eng.1", "1617", 2, 2) in parts
    assert ("matchsheet", "eng.1", "1617", 1, 1) in parts


def test_parity_includes_world_cup_with_calendar_season_label(legacy):
    db = legacy
    db.con.execute("""INSERT INTO iceberg.bronze.espn_match
        (competition_slug,season_year,event_id,kickoff,home_score,away_score,first_published_at)
        VALUES ('fifa.world',2018,1,'2018-07-15',4,2,'2026-01-01')""")
    db.con.execute("""INSERT INTO iceberg.bronze.espn_schedule VALUES
        ('INT-World Cup','2018','1','2018-07-15','4','2','final'),
        ('INT-World Cup','2018','2','2018-07-14','2','0','third')""")
    rows = db.run(render_parity_sql((("fifa.world", 2018),)))
    assert rows[0][:6] == ("fifa.world", 2018, "2018", 1, False, False)
    assert rows[0][9:12] == (False, False, False)
    assert rows[1][5] is True


def test_red_type_does_not_hide_missing_expected_type(db):
    scope = tuple(("eng.1", 1900 + i) for i in range(80))
    history.save_rows(db.con, [history.QueueRow(slug, year, 1,
                      history.RED if i == 0 else history.DONE)
                      for i, (slug, year) in enumerate(scope)])
    evidence = {pair: CoreEvidence(pair[1], ((1, ()),)) for pair in scope}
    evidence[scope[0]] = CoreEvidence(scope[0][1], ((1, ()), (2, ())))
    rows = db.run(render_acceptance_sql(scope, evidence, {pair: True for pair in scope}))
    assert rows[0][4] and not rows[0][12]
    assert "приёмка: нет" in render_acceptance(rows)


@pytest.mark.parametrize("attempts", [0, history.MAX_ATTEMPTS - 1, history.MAX_ATTEMPTS])
def test_retryable_red_pair_blocks_acceptance(db, attempts):
    scope = tuple(("eng.1", 1900 + i) for i in range(80))
    history.save_rows(db.con, [
        history.QueueRow(slug, year, 0, history.RED if i == 0 else history.EMPTY,
                         attempts=attempts if i == 0 else 0)
        for i, (slug, year) in enumerate(scope)
    ])
    evidence = {pair: CoreEvidence(pair[1], ()) for pair in scope[1:]}
    rows = db.run(render_acceptance_sql(scope, evidence, {pair: True for pair in scope}))
    exhausted = attempts >= history.MAX_ATTEMPTS
    assert rows[0][3] is not exhausted
    assert rows[0][12] is exhausted
    assert ("приёмка: да" in render_acceptance(rows)) is exhausted


def test_acceptance_includes_terminal_nonplayed_without_publication(db):
    pair = ("eng.1", 2016)
    history.save_rows(db.con, [history.QueueRow(*pair, 1, history.DONE)])
    db.match(1, season_year=2016, status="STATUS_CANCELED", played_final=False,
             terminal_nonplayed=True)
    rows = db.run(render_acceptance_sql(
        (pair,), {pair: CoreEvidence(2016, ((1, (1,)),))}, {pair: True}))
    assert rows[0][9:12] == (1, 0, 0)
    assert "приёмка: да" in render_acceptance(rows)


def test_parity_includes_terminal_nonplayed_without_publication(legacy):
    legacy.con.execute("""INSERT INTO iceberg.bronze.espn_match
        (competition_slug,season_year,event_id,status,played_final,terminal_nonplayed)
        VALUES ('eng.1',2016,1,'STATUS_CANCELED',false,true)""")
    legacy.con.execute("""INSERT INTO iceberg.bronze.espn_schedule VALUES
        ('ENG-Premier League','1617','1','2016-08-20',NULL,NULL,'canceled')""")
    rows = legacy.run(render_parity_sql((("eng.1", 2016),)))
    assert len(rows) == 1
    assert rows[0][3:6] == (1, False, False)
    assert rows[0][10] is None


@pytest.mark.parametrize("slug,league,year,label,dates", [
    ("eng.1", "ENG-Premier League", 2016, "1516",
     ("2016-06-30", "2016-07-01", "2017-06-30", "2017-07-01")),
    ("fifa.world", "INT-World Cup", 2018, "2014",
     ("2017-12-31", "2018-01-01", "2018-12-31", "2019-01-01")),
])
def test_parity_keeps_legacy_only_with_wrong_label_by_date(legacy, slug, league, year, label, dates):
    for event_id, date in enumerate(dates):
        legacy.con.execute("INSERT INTO iceberg.bronze.espn_schedule VALUES (?, ?, ?, ?, ?, ?, ?)",
                           [league, label, str(event_id), date, None, None, str(event_id)])
    rows = legacy.run(render_parity_sql(((slug, year),)))
    assert [r[3] for r in rows] == [1, 2]
    assert all(r[1] is None and r[2] == label and r[5] and r[8] for r in rows)
    assert all(r[12] == [year] for r in rows)
    assert f"date_seasons=[{year}]" in render_parity(rows)


@pytest.mark.parametrize("event_id", ["1", None])
def test_parity_checks_all_dates_of_ambiguous_legacy_key(legacy, event_id):
    for date in ("2016-06-30", "2016-08-20"):
        legacy.con.execute("INSERT INTO iceberg.bronze.espn_schedule VALUES (?, ?, ?, ?, ?, ?, ?)",
                           ["ENG-Premier League", "1516", event_id, date, None, None, "a"])
    rows = legacy.run(render_parity_sql((("eng.1", 2016),)))
    assert len(rows) == 1
    assert rows[0][5] and rows[0][7:9] == (2, True)
    assert rows[0][12] == [2016]


@pytest.mark.parametrize("assessment", [{}, {("eng.1", 2016): None}])
def test_unknown_quality_blocks_otherwise_complete_acceptance(db, assessment):
    scope = tuple(("eng.1", year) for year in range(2016, 2022))
    history.save_rows(db.con, [history.QueueRow(*pair, 0, history.EMPTY) for pair in scope])
    evidence = {pair: CoreEvidence(pair[1], ()) for pair in scope}
    rows = db.run(render_acceptance_sql(scope, evidence, assessment))
    assert "приёмка: нет" in render_acceptance(rows)
    assert rows[0][13] is True


def test_quality_assessment_is_required():
    with pytest.raises(TypeError):
        render_acceptance_sql((("eng.1", 2016),), {})


@pytest.mark.parametrize("published_duplicate", [False, True])
def test_physical_bronze_duplicates_make_complete_pair_red(db, published_duplicate):
    pair = ("eng.1", 2016)
    history.save_rows(db.con, [history.QueueRow(*pair, 1, history.DONE)])
    evidence = {pair: CoreEvidence(2016, ((1, (1,)),))}
    db.match(1, season_year=2016, first_published_at="2026-01-01")
    db.match(1, season_year=2016,
             first_published_at="2026-01-01" if published_duplicate else None)
    # Unknown quality must not mask independently confirmed uniqueness defects.
    rows = db.run(render_acceptance_sql((pair,), evidence, {}))
    assert rows[0][4] is True
    assert rows[0][9:12] == (1, 0, 0)
    assert rows[0][14] == 1
    assert "bronze_duplicates=1" in render_acceptance(rows)


@pytest.mark.parametrize("part", ["lineup", "matchsheet"])
@pytest.mark.parametrize("link", ["native", "date"])
def test_legacy_parts_keep_wrong_label_via_schedule_scope(legacy, part, link):
    legacy.con.execute("""INSERT INTO iceberg.bronze.espn_match
        (competition_slug,season_year,event_id,first_published_at)
        VALUES ('eng.1',2016,1,'2026-01-01')""")
    legacy.con.execute("INSERT INTO iceberg.bronze.espn_schedule VALUES "
                       "('ENG-Premier League','1516',?,?,NULL,NULL,'a')",
                       ['1' if link == 'native' else '2', None if link == 'native' else '2016-08-20'])
    # Multiple parts and a multi-season scope must not multiply report rows.
    legacy.con.execute(f"INSERT INTO iceberg.bronze.espn_{part} VALUES "
                       "('ENG-Premier League','1516','a'),('ENG-Premier League','1516','a'),"
                       "('ENG-Premier League','1415','outside')")
    rows = legacy.run(render_legacy_parts_sql((("eng.1", 2016), ("eng.1", 2017))))
    assert (part, "eng.1", "1516", 2, 0) in rows
    assert all(row[2] != '1415' for row in rows)
