"""Recorded responses and constructed failure cases for T4/T5/T6 (#2405)."""

import gzip
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from scrapers.transfermarkt import core, injury_sections as sections

pytestmark = pytest.mark.unit
FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "transfermarkt"
RECORDED = FIXTURES / "injury_sections"
TM = "https://www.transfermarkt.com"
HISTORY = TM + "/erling-haaland/verletzungen/spieler/418560"
INJURIES = "https://tmapi.transfermarkt.technology/player/28003/injury"
CLUB = TM + "/arsenal-fc/sperrenundverletzungen/verein/11"
EXTRACTED = "2026-11-23T12:00:00.000000+00:00"


def cap(endpoint, url, body, at=1, http=200):
    if not isinstance(body, bytes):
        body = (body if isinstance(body, str) else json.dumps(body)).encode()
    clock = f"2026-11-22T00:00:{at:02d}.000000+00:00"
    cap_id = hashlib.sha256((url + clock).encode() + body).hexdigest()
    return core.SourceCapture(
        url, endpoint, body, cap_id, hashlib.sha256(body).hexdigest(), clock, http
    )


def extract(*captures, **scope):
    return sections.extract(
        captures,
        core.ScopeContext("GB1", "2627", "scope", "cycle", **scope),
        extracted_at=EXTRACTED,
        universe_version="v1",
    )


def actual_current_files():
    root = FIXTURES / "stage_a_rewrite"
    for f in sorted((root / "captures" / "current_injury").glob("*.json")):
        meta = json.loads(f.read_bytes())
        yield (
            meta,
            gzip.decompress(
                (root / "blobs" / Path(meta["blob_key"]).name).read_bytes()
            ),
        )


def fixture_payload():
    return json.loads(
        gzip.decompress((RECORDED / "player_28003_injury.json.gz").read_bytes())
    )


def box(title, headers, rows):
    return (
        f'<div class="box"><h2 class="content-box-headline">{title}</h2><table class="items">'
        "<thead><tr>"
        + "".join(f"<th>{h}</th>" for h in headers)
        + "</tr></thead><tbody>"
        + "".join(
            "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
            for row in rows
        )
        + "</tbody></table></div>"
    )


HIST_HEAD = ("Season", "Injury", "from", "until", "Days", "Games missed")
ABS_HEAD = ("Player", "Age", "Reason", "since", "Expected return", "Missed matches")
RISK_HEAD = ("Player", "Age", "Competition", "Yellow cards", "Appearances")


def history_page(page, last, rows, *, nav=True):
    url = HISTORY if page == 1 else f"{HISTORY}/page/{page}"
    body = f'<meta property="og:url" content="{url}"><tm-subnavigation controller="spieler" id="418560"></tm-subnavigation>'
    markup = box("Injury history", HIST_HEAD, rows)
    if nav:

        def link(n, cls=""):
            href = HISTORY if n == 1 else f"{HISTORY}/page/{n}"
            return f'<li class="tm-pagination__list-item {cls}"><a href="{href}">{n}</a></li>'

        pages = link(page, "tm-pagination__list-item--active")
        if page < last:
            pages += link(page + 1, "tm-pagination__list-item--icon-next-page")
            pages += link(last, "tm-pagination__list-item--icon-last-page")
        if page > 1:
            pages += link(page - 1, "tm-pagination__list-item--icon-previous-page")
        markup = markup.replace(
            "</table>",
            '</table><div class="pager"><ul class="tm-pagination">'
            + pages
            + "</ul></div>",
        )
    return cap("player_injury_history_html", url, body + markup, page)


def episodes(start, count):
    return [
        [
            "25/26",
            "Knee injury",
            (date(2025, 1, 1) + timedelta(days=i)).strftime("%d/%m/%Y"),
            "-",
            "-",
            "-",
        ]
        for i in range(start, start + count)
    ]


def player(pid):
    return f'<a href="/name/profil/spieler/{pid}">Player {pid}</a>'


def club_capture(absences, risks, *, http=200):
    body = f'<meta property="og:url" content="{CLUB}"><tm-subnavigation controller="verein" id="11"></tm-subnavigation>'
    body += box("Suspensions and injuries", ABS_HEAD, absences)
    body += box("Risk of suspension", RISK_HEAD, risks)
    return cap("club_absences", CLUB, body, http=http)


def test_four_recorded_current_editions_match_source_and_exclude_league_standings():
    expected = {
        "URU1": ("2025", 28),
        "IRN2": ("2026", 8),
        "SA2L": ("2026", 10),
        "UR2T": ("2025", 12),
    }
    counts = {}
    for meta, body in actual_current_files():
        comp = meta["url"].split("/")[-1]
        edition, expected_count = expected[comp]
        source = cap("current_injury", meta["url"], body)
        (out,) = extract(source, competition_id=comp, edition_id=edition)
        assert out.status == "observed_present" and len(out.rows) == expected_count
        assert all(
            r["competition_id"] == comp
            and r["edition_id"] == edition
            and r["since"] is None
            and r["raw_capture_id"] == source.capture_id
            for r in out.rows
        )
        counts[comp] = len(out.rows)
        if comp == "URU1":
            row = out.rows[0]
            assert (
                row["player_id"],
                row["club_id"],
                row["injury"],
                row["expected_return"],
            ) == (
                "115507",
                "866",
                "Muscle tear",
                "2026-11-27",
            )
        (bad,) = extract(source, competition_id=comp, edition_id="2000")
        assert bad.status == "error" and not bad.rows
    assert set(counts) == set(expected)


def test_json_all_50_episodes_and_open_correction_keep_source_clocks_and_keys():
    raw = fixture_payload()
    (first,) = extract(cap("player_injury_history", INJURIES, raw), player_id="28003")
    assert first.status == "observed_present" and len(first.rows) == 50
    assert sum(r["days"] for r in first.rows) == 865
    assert sum(r["games_missed"] for r in first.rows) == 163
    assert len({r["injury_id"] for r in first.rows}) == 32
    old = first.rows[0]
    assert (old["injury"], old["season_id"], old["from_date"], old["until_date"]) == (
        "muscular problems",
        2025,
        "2026-05-26",
        "2026-06-06",
    )
    raw["data"]["injuries"][0].update(
        end=None, durationDetails=None, missedGamesCount=None
    )
    (corrected,) = extract(
        cap("player_injury_history", INJURIES, raw, at=2), player_id="28003"
    )
    new = corrected.rows[0]
    assert new["episode_key"] == old["episode_key"]
    assert (
        new["observed_at"] != old["observed_at"]
        and new["until_date"] is new["days"] is new["games_missed"] is None
    )


def test_json_requires_positive_envelope_row_and_full_collection():
    doc = fixture_payload()
    doc["data"]["injuries"][-1]["playerId"] = "1"
    for bad in (
        doc,
        {"success": False, "data": {"playerId": "28003", "injuries": []}},
        {"success": True, "data": {"playerId": "9", "injuries": []}},
        {"success": True, "data": {"playerId": "28003", "injuries": [], "nextPage": 2}},
    ):
        (out,) = extract(cap("player_injury_history", INJURIES, bad), player_id="28003")
        assert out.status == "error" and not out.rows
    (out,) = extract(
        cap(
            "player_injury_history",
            INJURIES,
            {"success": True, "data": {"playerId": "28003", "injuries": []}},
        ),
        player_id="28003",
    )
    assert out.status == "observed_empty" and out.proof
    (out,) = extract(
        cap("player_injury_history", INJURIES, {}, http=429), player_id="28003"
    )
    assert out.status == "unavailable" and not out.proof


def test_recorded_html_first_page_announces_second_and_cannot_be_committed_alone():
    recorded = cap(
        "player_injury_history_html",
        HISTORY,
        (RECORDED / "player_418560_page1.html").read_bytes(),
    )
    assert sections.next_page(recorded, 1) == HISTORY + "/page/2"
    (out,) = extract(recorded, player_id="418560")
    assert out.status == "error" and not out.rows


def test_four_html_pages_preserve_15_15_15_5_and_distinct_lineage():
    pages = [
        history_page(n, 4, episodes((n - 1) * 15, 15 if n < 4 else 5))
        for n in range(1, 5)
    ]
    outcomes = extract(*pages, player_id="418560")
    assert [len(o.rows) for o in outcomes] == [15, 15, 15, 5]
    assert len({r["episode_key"] for o in outcomes for r in o.rows}) == 50
    assert len({o.run_id for o in outcomes}) == 1
    for n, (source, out) in enumerate(zip(pages, outcomes, strict=True), 1):
        assert all(
            r["raw_capture_id"] == source.capture_id
            and r["source_url"] == source.url
            and r["observed_at"] == source.captured_at
            and r["page_number"] == n
            for r in out.rows
        )
        plan = core.plan_for_outcome(
            out, captured_at=source.captured_at, scope_values={"player_id": "418560"}
        )
        assert plan.write_mode == "merge"
    for incomplete in (
        pages[:-1],
        pages[1:],
        [*pages[:2], pages[3]],
        [pages[0], pages[0], *pages[1:]],
    ):
        (fail,) = extract(*incomplete, player_id="418560")
        assert fail.status == "error" and not fail.rows


def test_wrong_player_link_header_http_and_overlapping_final_page_fail_whole_walk():
    one = history_page(1, 2, episodes(0, 15))
    two = history_page(2, 2, episodes(15, 1))
    for body in (
        two.text.replace("418560", "445"),
        two.text.replace("Injury history", "Total"),
        two.text.replace("16/01/2025", "01/01/2025"),
    ):
        (fail,) = extract(
            one, cap(two.endpoint, two.url, body, at=2), player_id="418560"
        )
        assert fail.status == "error" and not fail.rows
    (fail,) = extract(
        one, cap(two.endpoint, two.url, "error", at=2, http=502), player_id="418560"
    )
    assert (
        fail.status == "unavailable" and not fail.rows and fail.capture.url == two.url
    )
    crossed = cap(
        one.endpoint, one.url, one.text.replace("/418560/page/2", "/234/page/2")
    )
    with pytest.raises(ValueError):
        sections.next_page(crossed, 1)
    (fail,) = extract(crossed, two, player_id="418560")
    assert fail.status == "error" and not fail.rows


def test_club_current_separates_sanctions_other_absences_injuries_and_risks():
    competition = (
        '<a href="/premier-league/startseite/wettbewerb/GB1">Premier League</a>'
    )
    capture = club_capture(
        [
            [player(1), "23", "Red card suspension", "01/10/2026", "2 matches", "1"],
            [player(2), "22", "National team call-up", "02/10/2026", "?", "-"],
            [player(3), "28", "Personal leave", "-", "-", "-"],
            [player(4), "25", "Cruciate ligament tear", "-", "-", "-"],
        ],
        [[player(1), "23", competition, "4", "12"]],
    )
    absences, risks = extract(capture, club_id="11")
    assert [(r["player_id"], r["absence_kind"]) for r in absences.rows] == [
        ("1", "suspension"),
        ("2", "national_callup"),
        ("3", "other"),
    ]
    assert absences.rows[0]["end_condition"] == "2 matches"
    assert (
        absences.rows[0]["until_date"] is None
        and absences.rows[1]["until_date"] is None
    )
    assert absences.rows[2]["since"] is None
    assert [
        (r["player_id"], r["competition_id"], r["yellow_cards"]) for r in risks.rows
    ] == [("1", "GB1", 4)]
    assert not any(r["player_id"] == "4" for r in (*absences.rows, *risks.rows))
    assert "injuries=1" in absences.status_reason
    fail = extract(
        cap("club_absences", CLUB, capture.text.replace('id="11"', 'id="19"')),
        club_id="11",
    )
    assert all(o.status == "error" and not o.rows for o in fail)


@pytest.mark.parametrize(
    "endpoint,url",
    [
        (
            "current_injury",
            TM + "/premier-league/verletztespieler/wettbewerb/GB1?season=2015",
        ),
        ("player_injury_history", INJURIES + "?page=2"),
        ("player_injury_history", INJURIES.replace("/28003/", "/0/")),
        ("player_injury_history_html", HISTORY + "/page/02"),
        ("player_injury_history_html", HISTORY + "?target=elsewhere"),
        (
            "club_absences",
            CLUB.replace("www.transfermarkt.com", "www.transfermarkt.com.example.org"),
        ),
        ("club_absences", CLUB + "/saison_id/2015"),
        ("club_absences", CLUB + "#history"),
    ],
)
def test_scope_rejects_wrong_urls(endpoint, url):
    with pytest.raises(ValueError):
        sections.scope_from_url(endpoint, url, edition_id="2026")


def test_scope_extracts_only_evidenced_targets():
    assert sections.scope_from_url("player_injury_history", INJURIES) == {
        "player_id": "28003"
    }
    assert sections.scope_from_url("club_absences", CLUB) == {"club_id": "11"}
    assert sections.scope_from_url(
        "current_injury",
        TM + "/fa-cup/verletztespieler/pokalwettbewerb/FACC",
        competition_id="FACC",
        edition_id="2026",
    ) == {"competition_id": "FACC", "edition_id": "2026"}
    assert sections.page_number("player_injury_history_html", HISTORY + "/page/2") == 2


def test_recorded_html_keeps_two_affected_teams_one_game_total_and_no_season_summaries():
    # Only page 1 is a recording. A constructed tail exercises the recorded summary of 23 episodes.
    source = cap(
        "player_injury_history_html",
        HISTORY,
        (RECORDED / "player_418560_page1.html").read_bytes(),
    )
    first, second = extract(
        source, history_page(2, 2, episodes(0, 8)), player_id="418560"
    )
    assert [len(first.rows), len(second.rows)] == [15, 8]
    assert (
        first.rows[0]["injury"],
        first.rows[0]["from_date"],
        first.rows[0]["games_missed"],
    ) == (
        "Knock",
        "2026-02-26",
        1,
    )
    multi = first.rows[7]
    assert multi["games_missed"] == 3 and json.loads(multi["affected_clubs_json"]) == [
        {"club_id": "281", "club_name": "Manchester City"},
        {"club_id": "3440", "club_name": "Norway"},
    ]
    (lost,) = extract(source, history_page(2, 2, episodes(0, 7)), player_id="418560")
    assert lost.status == "error" and "history_totals_mismatch" in lost.status_reason


def test_a_newer_page_cannot_shortcut_the_last_page_already_announced():
    first = history_page(1, 4, episodes(0, 15))
    last = history_page(2, 2, episodes(15, 1))
    (out,) = extract(first, last, player_id="418560")
    assert (
        out.status == "error" and "history_announced_pages_missing" in out.status_reason
    )


def test_missing_unknown_and_explicit_empty_current_tables_stay_distinct():
    meta, body = next(actual_current_files())
    soup = BeautifulSoup(body, "html.parser")
    table = soup.select_one("table.items")
    table.tbody.clear()
    marker = soup.new_tag("tr")
    cell = soup.new_tag("td", colspan="5")
    cell.string = "No entries found."
    marker.append(cell)
    table.tbody.append(marker)
    context = {"competition_id": "URU1", "edition_id": "2025"}

    def read():
        return extract(cap("current_injury", meta["url"], str(soup)), **context)[0]

    empty = read()
    assert empty.status == "observed_empty" and empty.proof
    cell.string = "No information available"
    assert read().status == "error"
    table.decompose()
    assert read().status == "error"


def test_unknown_absence_reason_cannot_create_other_or_an_empty_sanction_table():
    source = club_capture(
        [[player(4), "20", "Potential new condition", "-", "-", "-"]],
        [[player(4), "20", "League A", "3", "5"]],
    )
    result = extract(source, club_id="11")
    assert all(
        out.status == "error" and "unclassified_absence_reason" in out.status_reason
        for out in result
    )


def test_full_history_bundle_writes_all_pages_and_replays_idempotently(
    monkeypatch, tmp_path
):
    from types import SimpleNamespace
    from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
    from scrapers.transfermarkt.orchestrator import (
        CoreRun,
        CoreScopePlan,
        SectionTarget,
    )
    from tests.unit.scrapers.test_transfermarkt_native_writer import CoreDB, CoreWriter

    monkeypatch.setenv("TM_WRITER_LOCK_PATH", str(tmp_path / "lock"))
    monkeypatch.setenv("TM_CORE_WRITE_JOURNAL_DIR", str(tmp_path / "journal"))
    monkeypatch.delenv("TM_WRITER_LOCK_SHARED_FILE_ID", raising=False)
    pages = [
        history_page(n, 4, episodes((n - 1) * 15, 15 if n < 4 else 5))
        for n in range(1, 5)
    ]
    by_url = {p.url: p for p in pages}
    requested = []

    def fetch(url, **options):
        requested.append(url)
        source = by_url[url]
        assert options["as_json"] is False
        return FetchOutcome(
            status=FetchStatus.OK,
            value=source.text,
            raw_capture_id=source.capture_id,
            raw_body_hash=source.body_hash,
            raw_fetched_at=source.captured_at,
            status_code=200,
        )

    def load(capture_id, url):
        source = by_url[url]
        assert source.capture_id == capture_id
        return source.body, SimpleNamespace(
            url=url,
            endpoint=source.endpoint,
            fetched_at=source.captured_at,
            content_hash=source.body_hash,
        )

    plan = CoreScopePlan(
        "GB1",
        "2627",
        "history",
        "cycle",
        section_targets=(SectionTarget(pages[0].endpoint, HISTORY),),
    )
    run = CoreRun(fetch, request_raw_loader=load, extracted_at=EXTRACTED)
    db = CoreDB()
    writer = CoreWriter(db)
    for iteration in range(2):
        report = run.run(plan, writer=writer, connection_factory=lambda: db)
        assert report.dq["passed"] and report.receipt["receipt_readback"]
        assert requested == [p.url for p in pages] * (iteration + 1)
        assert db.sql.execute(
            "SELECT page_number,count(*) FROM transfermarkt_player_injury_history_observations "
            "GROUP BY page_number ORDER BY page_number"
        ).fetchall() == [(1, 15), (2, 15), (3, 15), (4, 5)]
        assert db.sql.execute(
            "SELECT count(*) FROM transfermarkt_core_receipts_v1"
        ).fetchone() == (4,)
    bad = pages[1]
    by_url[bad.url] = cap(
        bad.endpoint, bad.url, bad.text.replace("418560", "418561"), at=2
    )
    other_db = CoreDB()
    result = run.run(
        plan, writer=CoreWriter(other_db), connection_factory=lambda: other_db
    )
    assert [(o["status"], o["rows"]) for o in result.outcomes] == [("error", 0)]
    assert not result.receipts and result.receipt is None
    assert not other_db.sql.execute(
        "SELECT name FROM sqlite_master WHERE name='transfermarkt_player_injury_history_observations'"
    ).fetchall()


def test_only_recognized_injuries_and_an_explicit_empty_risk_section_can_prove_empty():
    capture = club_capture([[player(42), "25", "Knee injury", "-", "-", "-"]], [])
    soup = BeautifulSoup(capture.body, "html.parser")
    risk_table = soup.select("table.items")[1]
    cell = soup.new_tag("td", colspan="5")
    cell.string = "No results found."
    row = soup.new_tag("tr")
    row.append(cell)
    risk_table.tbody.append(row)
    result = extract(cap(capture.endpoint, capture.url, str(soup)), club_id="11")
    assert [(o.status, bool(o.proof)) for o in result] == [
        ("observed_empty", True),
        ("observed_empty", True),
    ]
    risk_table.decompose()
    assert all(
        o.status == "error"
        for o in extract(cap(capture.endpoint, capture.url, str(soup)), club_id="11")
    )


def test_same_players_risks_keep_separate_competitions_and_same_scope_conflicts_fail():
    england = '<a href="/premier-league/startseite/wettbewerb/GB1">Premier League</a>'
    europe = (
        '<a href="/champions-league/startseite/pokalwettbewerb/CL">Champions League</a>'
    )
    absences = [[player(42), "25", "Suspension", "01/10/2026", "-", "1"]]
    first = [player(42), "25", england, "4", "12"]
    second = [player(42), "25", europe, "2", "4"]
    _, risks = extract(club_capture(absences, [first, second]), club_id="11")
    assert [(r["competition_id"], r["yellow_cards"]) for r in risks.rows] == [
        ("GB1", 4),
        ("CL", 2),
    ]
    conflict = [*first[:-2], "5", "12"]
    result = extract(club_capture(absences, [first, conflict]), club_id="11")
    assert all(o.status == "error" and not o.rows for o in result)


ABSENCE_HISTORY_URL = "https://tmapi.transfermarkt.technology/player/28003/absence"


def absence_history_payload():
    raw = gzip.decompress((RECORDED / "player_28003_absence.json.gz").read_bytes())
    assert (
        hashlib.sha256(raw).hexdigest()
        == "a67195460aa360f76d135b11fba0832aa00affb0f2b120dfa82168be630377a0"
    )
    return json.loads(raw)


def test_recorded_player_absences_preserve_both_competitions_and_no_club_is_inferred():
    from collections import Counter

    raw = absence_history_payload()
    source = cap("player_absence_history", ABSENCE_HISTORY_URL, raw)
    (out,) = extract(source, player_id="28003")
    assert out.status == "observed_present" and len(out.rows) == 17
    assert Counter(r["absence_kind"] for r in out.rows) == {
        "suspension": 10,
        "national_callup": 4,
        "other": 3,
    }
    assert sum(r["competition_id"] is None for r in out.rows) == 7
    assert all(
        r["club_id"] is None and r["raw_capture_id"] == source.capture_id
        for r in out.rows
    )
    assert {r["absence_id"] for r in out.rows} == {1, 3, 4, 5, 6, 8}
    assert sum(r["games_missed"] for r in out.rows) == 38
    assert sum(r["possible_missing_games"] for r in out.rows) == 17
    assert sum(r["days"] for r in out.rows) == 517
    same_start = [r for r in out.rows if r["since"] == "2021-01-17"]
    assert [(r["competition_id"], r["until_date"]) for r in same_start] == [
        ("ES1", "2021-01-25"),
        ("CDR", "2021-01-22"),
    ]
    assert len({r["absence_identity"] for r in same_start}) == 2
    plan = core.plan_for_outcome(
        out, captured_at=source.captured_at, scope_values={"player_id": "28003"}
    )
    assert plan.write_mode == "merge"


def test_historical_absence_changes_retain_open_dates_and_cannot_accept_risk_or_missing_scope():
    doc = absence_history_payload()
    (first,) = extract(
        cap("player_absence_history", ABSENCE_HISTORY_URL, doc), player_id="28003"
    )
    row = doc["data"]["absences"][0]
    row.update(
        end=None,
        durationDetails=None,
        missedGamesCount=None,
        possibleMissingGamesCount=None,
    )
    (second,) = extract(
        cap("player_absence_history", ABSENCE_HISTORY_URL, doc, at=2), player_id="28003"
    )
    record = second.rows[0]
    assert first.rows[0]["absence_identity"] == record["absence_identity"]
    assert first.rows[0]["observed_at"] != record["observed_at"]
    assert (
        record["until_date"]
        is record["days"]
        is record["games_missed"]
        is record["possible_missing_games"]
        is None
    )
    row["playerId"] = "234"
    (fail,) = extract(
        cap("player_absence_history", ABSENCE_HISTORY_URL, doc), player_id="28003"
    )
    assert fail.status == "error" and not fail.rows
    row["playerId"] = "28003"
    for reason in ("Risk of suspension", "Knee injury"):
        row["name"] = reason
        (fail,) = extract(
            cap("player_absence_history", ABSENCE_HISTORY_URL, doc), player_id="28003"
        )
        assert fail.status == "error" and not fail.rows
    empty = {
        "success": True,
        "data": {"playerId": "28003", "absences": [], "competitionIds": []},
    }
    (verified,) = extract(
        cap("player_absence_history", ABSENCE_HISTORY_URL, empty), player_id="28003"
    )
    assert verified.status == "observed_empty" and verified.proof
    empty["data"]["competitionIds"] = ["GB1"]
    (inconsistent,) = extract(
        cap("player_absence_history", ABSENCE_HISTORY_URL, empty), player_id="28003"
    )
    assert inconsistent.status == "error"


def test_verified_club_group_does_not_lose_sanctions_when_risk_is_unpublished():
    # Independently constructed from the two published section layouts. No third-party HTML fixture is copied.
    badge = '<img src="https://img.a.transfermarkt.technology/logo/tiny/xyz.png?lm=1" alt="Example Cup">'
    sample = club_capture(
        [
            [player(10), "20", "Newly described muscle condition", "-", "-", "1"],
            [
                player(11),
                "21",
                badge + "No eligibility",
                "01/10/2026",
                "21/10/2026",
                "3",
            ],
            [player(12), "22", badge + "Red card suspension", "01/10/2026", "-", "2"],
        ],
        [],
    )
    soup = BeautifulSoup(sample.body, "html.parser")
    first, risk = soup.select("table.items")
    rows = first.tbody.select(":scope > tr")
    for start, label in ((0, "Injuries"), (1, "Suspensions")):
        group = soup.new_tag("tr")
        cell = soup.new_tag("td", colspan="6", attrs={"class": "extrarow"})
        cell.string = label
        group.append(cell)
        rows[start].insert_before(group)
    grid = soup.new_tag("div", attrs={"class": "grid-view"})
    no_data = soup.new_tag("span", attrs={"class": "empty"})
    no_data.string = "No information available."
    grid.append(no_data)
    risk.replace_with(grid)
    absent, risk_out = extract(cap("club_absences", CLUB, str(soup)), club_id="11")
    assert [
        (
            r["source_group"],
            r["reason_raw"],
            r["absence_kind"],
            r["competition_name"],
            r["competition_id"],
        )
        for r in absent.rows
    ] == [
        ("Suspensions", "No eligibility", "other", "Example Cup", None),
        ("Suspensions", "Red card suspension", "suspension", "Example Cup", None),
    ]
    assert all(r["competition_logo_url"].endswith("/xyz.png?lm=1") for r in absent.rows)
    assert (
        "injuries=1" in absent.status_reason
        and risk_out.status == "unknown"
        and not risk_out.proof
    )
    rows[0].find_all("td", recursive=False)[2].string = "Red card suspension"
    assert all(
        o.status == "error"
        for o in extract(cap("club_absences", CLUB, str(soup)), club_id="11")
    )


def test_published_card_rate_and_unpublished_absence_grid_are_independent():
    body = f'<meta property="og:url" content="{CLUB}">'
    body += '<div class="box"><h2 class="content-box-headline">Suspensions and injuries</h2>'
    body += '<div class="grid-view"><span class="empty">No information available.</span></div></div>'
    body += box(
        "Risk of suspension",
        ("Player", "Age", "Yellow cards", "Appearances", "Cards/Match"),
        [[player(99), "23", "4", "7", "0.57"]],
    )
    absent, risk = extract(cap("club_absences", CLUB, body), club_id="11")
    assert absent.status == "unknown" and not absent.proof
    (row,) = risk.rows
    assert (
        row["player_id"],
        row["competition_id"],
        row["yellow_cards"],
        row["appearances"],
        row["cards_per_match_raw"],
    ) == ("99", None, 4, 7, "0.57")


def test_player_absence_history_keeps_corrected_and_original_observations_in_physical_readback(
    tmp_path, monkeypatch
):
    from scrapers.transfermarkt import native_writer
    from tests.unit.scrapers.test_transfermarkt_native_writer import CoreDB, CoreWriter

    monkeypatch.setenv("TM_WRITER_LOCK_PATH", str(tmp_path / "lock"))
    monkeypatch.setenv("TM_CORE_WRITE_JOURNAL_DIR", str(tmp_path / "journal"))
    monkeypatch.delenv("TM_WRITER_LOCK_SHARED_FILE_ID", raising=False)
    raw = absence_history_payload()
    db = CoreDB()
    writer = CoreWriter(db)
    for observed in (1, 2):
        if observed == 2:
            raw["data"]["absences"][0].update(
                end=None, durationDetails=None, missedGamesCount=None
            )
        source = cap("player_absence_history", ABSENCE_HISTORY_URL, raw, at=observed)
        scope = core.ScopeContext(
            "GB1", "2627", "global-player", f"pass-{observed}", player_id="28003"
        )
        (out,) = sections.extract(
            [source], scope, extracted_at=EXTRACTED, universe_version="v1"
        )
        plan = core.plan_for_outcome(
            out, captured_at=source.captured_at, scope_values={"player_id": "28003"}
        )
        receipt = native_writer.write_core_bundle(
            lambda: db,
            writer,
            [plan],
            run_id=out.run_id,
            universe_version="v1",
            committed_at=EXTRACTED,
        )
        assert receipt["receipt_readback"] and receipt["verified"]
    assert db.sql.execute(
        "SELECT count(*),count(club_id) FROM transfermarkt_player_absence_history_observations"
    ).fetchone() == (34, 0)
    assert db.sql.execute(
        "SELECT until_date,games_missed FROM transfermarkt_player_absence_history_observations "
        "WHERE since='2026-07-20' ORDER BY observed_at"
    ).fetchall() == [("2026-07-31", 2), (None, None)]
