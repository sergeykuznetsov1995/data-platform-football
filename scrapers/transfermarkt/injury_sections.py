"""T4 current injured players, T5 published injury episodes, T6 current absences.

The HTML history selects the Injury history table and published page links as
in omkarcloud/transfermarkt-scraper, players.get_injuries at ad660278c0c95bd46c2d0553658fcd14ebd9cb8f (MIT,
licenses/transfermarkt-scraper-omkarcloud-MIT.txt). It reconciles every source
row and walk before emitting per-capture observations. Season totals never
become episodes. The club absence page has no historical contract.
"""

from __future__ import annotations

from datetime import date
import json
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from . import current_injuries as current
from .injury_source import InjurySectionError, absence_kind, api_history, identity
from .section_contracts import ObservationTable, make_outcome


_CURRENT = "current_injury_observations"
_HISTORY = "player_injury_history_observations"
_ABSENCES = "club_absence_observations"
_RISKS = "club_suspension_risk_observations"
_ABSENCE_HISTORY = "player_absence_history_observations"
TABLES = {
    _CURRENT: ObservationTable(
        current.CURRENT_INJURY_COLUMNS,
        current.CURRENT_INJURY_OBSERVATION_KEY,
        ("competition_id", "edition_id"),
        {"since": "date", "expected_return": "date"},
    ),
    _HISTORY: ObservationTable(
        (
            "player_id",
            "episode_key",
            "injury",
            "injury_id",
            "category",
            "season_id",
            "season_label",
            "from_date",
            "until_date",
            "days",
            "duration_days",
            "games_missed",
            "affected_clubs_json",
            "page_number",
            "source_next_url",
        ),
        ("player_id", "injury", "from_date", "observed_at"),
        ("player_id",),
        {
            "injury_id": "bigint",
            "season_id": "bigint",
            "from_date": "date",
            "until_date": "date",
            "days": "bigint",
            "duration_days": "bigint",
            "games_missed": "bigint",
            "page_number": "bigint",
        },
    ),
    _ABSENCE_HISTORY: ObservationTable(
        (
            "player_id",
            "club_id",
            "competition_id",
            "absence_identity",
            "absence_id",
            "absence_kind",
            "reason_raw",
            "season_id",
            "season_label",
            "since",
            "until_date",
            "days",
            "duration_days",
            "games_missed",
            "possible_missing_games",
        ),
        ("player_id", "absence_identity", "observed_at"),
        ("player_id",),
        {
            "absence_id": "bigint",
            "season_id": "bigint",
            "since": "date",
            "until_date": "date",
            "days": "bigint",
            "duration_days": "bigint",
            "games_missed": "bigint",
            "possible_missing_games": "bigint",
        },
    ),
    _ABSENCES: ObservationTable(
        (
            "club_id",
            "player_id",
            "absence_kind",
            "absence_identity",
            "reason_raw",
            "source_group",
            "competition_logo_url",
            "competition_id",
            "competition_name",
            "since",
            "until_date",
            "end_condition",
            "games_missed",
        ),
        ("club_id", "player_id", "absence_kind", "absence_identity", "observed_at"),
        ("club_id",),
        {"since": "date", "until_date": "date", "games_missed": "bigint"},
    ),
    _RISKS: ObservationTable(
        (
            "club_id",
            "player_id",
            "competition_key",
            "competition_id",
            "competition_name",
            "yellow_cards",
            "appearances",
            "cards_per_match_raw",
        ),
        ("club_id", "player_id", "competition_key", "observed_at"),
        ("club_id",),
        {"yellow_cards": "bigint", "appearances": "bigint"},
    ),
}
ENDPOINT_ENTITIES = {
    "current_injury": (_CURRENT,),
    "player_injury_history": (_HISTORY,),
    "player_injury_history_html": (_HISTORY,),
    "club_absences": (_ABSENCES, _RISKS),
    "player_absence_history": (_ABSENCE_HISTORY,),
}
JSON_ENDPOINTS = frozenset({"player_injury_history", "player_absence_history"})
PAGINATED = frozenset({"player_injury_history_html"})
_TM = r"https://www\.transfermarkt\.com/[A-Za-z0-9_-]+/"
_ID = r"([1-9][0-9]*)"
_ROUTES = {
    "current_injury": re.compile(
        _TM + r"verletztespieler/(?:wettbewerb|pokalwettbewerb)/([A-Z0-9_-]+)/?"
    ),
    "player_absence_history": re.compile(
        r"https://tmapi\.transfermarkt\.technology/player/" + _ID + "/absence"
    ),
    "player_injury_history": re.compile(
        r"https://tmapi\.transfermarkt\.technology/player/" + _ID + "/injury"
    ),
    "player_injury_history_html": re.compile(
        _TM + "verletzungen/spieler/" + _ID + r"(?:/page/([1-9][0-9]*))?/?"
    ),
    "club_absences": re.compile(_TM + "sperrenundverletzungen/verein/" + _ID + "/?"),
}
_PLAYER_LINK = re.compile(r"/[A-Za-z0-9_-]+/profil/spieler/" + _ID + r"/?")
_CLUB_LINK = re.compile(
    r"/[A-Za-z0-9_-]+/startseite/verein/" + _ID + r"(?:/saison_id/[0-9]{4})?/?"
)
_COMP_LINK = re.compile(
    r"/[A-Za-z0-9_-]+/(?:startseite|spielplan)/(?:wettbewerb|pokalwettbewerb)/([A-Z0-9_-]+)"
    r"(?:/saison_id/[0-9]{4})?/?"
)
_EMPTY = {"no entries available", "no entries found", "no results found"}
_MISSING = {"", "-", "–", "?", "unknown"}
_REVISION = "injuries-absences-section-v1"


def _route(endpoint, url):
    route = _ROUTES.get(endpoint)
    match = route.fullmatch(url) if route is not None and isinstance(url, str) else None
    if not match:
        raise InjurySectionError("invalid_injury_section_url")
    return match


def scope_from_url(endpoint, url, *, edition_id="", competition_id=""):
    target = _route(endpoint, url)[1]
    if endpoint == "current_injury":
        if competition_id and competition_id != target:
            raise InjurySectionError("competition_mismatch")
        return {"competition_id": target, "edition_id": current._edition_id(edition_id)}
    return {"club_id" if endpoint == "club_absences" else "player_id": target}


def page_number(endpoint, url):
    match = _route(endpoint, url)
    return int(match[2] or "1") if endpoint in PAGINATED else 1


def _source_identity(capture, soup, edition=None):
    """Check the answered page independently of the request URL, including empty pages."""
    endpoint = capture.endpoint
    target = _route(endpoint, capture.url)[1]
    controllers = {
        "current_injury": {"wettbewerb", "pokalwettbewerb", "pokal"},
        "player_injury_history_html": {"spieler"},
        "club_absences": {"verein"},
    }
    navs = soup.select("tm-subnavigation")
    if any(
        n.get("id") != target or n.get("controller") not in controllers[endpoint]
        for n in navs
    ):
        raise InjurySectionError("answered_target_mismatch")
    if endpoint == "current_injury" and (
        not navs or any(n.get("season") != edition for n in navs)
    ):
        raise InjurySectionError("answered_edition_mismatch")
    labels = [n.get("content") for n in soup.select('meta[property="og:url"]')]
    labels.extend(n.get("href") for n in soup.select("link[rel~=canonical]"))
    if not labels and not navs:
        raise InjurySectionError("answered_target_missing")
    for label in labels:
        if _route(endpoint, label)[1] != target:
            raise InjurySectionError("answered_target_mismatch")
        if page_number(endpoint, label) not in {1, page_number(endpoint, capture.url)}:
            raise InjurySectionError("answered_page_mismatch")


def _block(soup, titles):
    blocks = [
        heading.find_parent(class_="box")
        for heading in soup.find_all(
            ["h1", "h2", "h3", "div"], class_="content-box-headline"
        )
        if _text(heading).lower() in titles
    ]
    if len(blocks) != 1 or blocks[0] is None:
        raise InjurySectionError("section_heading_missing_or_ambiguous")
    return blocks[0]


def _text(node):
    return " ".join(node.get_text(" ", strip=True).split())


def _table(block, *, group_rows=False):
    tables = block.select("table.items")
    if len(tables) != 1:
        raise InjurySectionError("section_table_missing_or_ambiguous")
    (table,) = tables
    heads = table.find_all("thead", recursive=False)
    bodies = table.find_all("tbody", recursive=False)
    if len(heads) != 1 or len(bodies) != 1:
        raise InjurySectionError("section_table_shape")
    headers = tuple(_text(c).casefold() for c in heads[0].find_all("th"))
    rows = bodies[0].find_all("tr", recursive=False)
    if not rows or not headers or len(set(headers)) != len(headers):
        raise InjurySectionError("section_table_shape")
    if len(rows) == 1:
        cells = rows[0].find_all("td", recursive=False)
        if (
            len(cells) == 1
            and cells[0].get("colspan") == str(len(headers))
            and "extrarow" not in cells[0].get("class", [])
            and _text(cells[0]).lower().rstrip(".") in _EMPTY
        ):
            return headers, []
    for row in rows:
        cells = row.find_all("td", recursive=False)
        if len(cells) == len(headers):
            continue
        if (
            group_rows
            and len(cells) == 1
            and "extrarow" in cells[0].get("class", [])
            and cells[0].get("colspan") == str(len(headers))
        ):
            continue
        raise InjurySectionError("section_row_shape")
    return headers, rows


def _without_pager(block):
    if block.select_one(".tm-pagination, .pagination, .pager a, [rel~=next]"):
        raise InjurySectionError("unhandled_section_pagination")


def _current_records(soup, scope):
    block = _block(soup, {"injured players"})
    _without_pager(block)
    headers, rows = _table(block)
    # The four recorded competition pages publish "until" and omit "since".
    fields = {
        "player/position": "player",
        "until": "expected_return",
        "market value": "market_value",
        **current._HEADERS,
    }
    mapped = [fields.get(key) for key in headers]
    if (
        None in mapped
        or len(set(mapped)) != len(mapped)
        or not {"player", "club", "injury", "expected_return"} <= set(mapped)
    ):
        raise InjurySectionError("current_injury_headers")
    records = [
        current._row(row, mapped, scope.competition_id, scope.edition_id).as_dict()
        for row in rows
    ]
    if any(
        record["player_id"] == "0" or record["club_id"] == "0" for record in records
    ):
        raise InjurySectionError("invalid_current_injury_identity")
    grid = block.select_one("table.items").find_parent(class_="grid-view")
    if grid is not None:
        inventories = grid.select(":scope > .keys")
        if len(inventories) > 1:
            raise InjurySectionError("current_injury_inventory_ambiguous")
        if inventories:
            published = [
                tag.get_text(strip=True)
                for tag in inventories[0].select(":scope > span")
            ]
            if published != list(inventories[0].stripped_strings) or published != [
                r["player_id"] for r in records
            ]:
                raise InjurySectionError("current_injury_inventory_mismatch")
    return records


def _number(cell, units=""):
    text = _text(cell)
    if text.lower() in _MISSING:
        return None
    match = re.fullmatch(
        r"([0-9]+)" + (r"(?:\s+(?:" + units + "))?" if units else ""), text, re.I
    )
    if not match or len(match[1]) > 18:
        raise InjurySectionError("invalid_published_number")
    return int(match[1])


def _links(cell, pattern):
    results = {}
    for link in cell.select("a[href]"):
        match = pattern.fullmatch(link["href"])
        if not match:
            raise InjurySectionError("invalid_entity_link")
        image = link.find("img")
        label = link.get("title") or (image and image.get("alt")) or _text(link) or None
        prior = results.setdefault(match[1], label)
        if prior and label and prior != label:
            raise InjurySectionError("conflicting_entity_names")
    return results


def _games_and_clubs(cell):
    anchors = cell.select("a[href]")
    if not anchors:
        return _number(cell), None
    # A player can miss games for several clubs in a single episode.
    clubs = _links(cell, _CLUB_LINK)
    counts = cell.find_all("span", recursive=False)
    if (
        len(clubs) != len(anchors)
        or len(counts) != 1
        or anchors[-1].find_next_sibling() is not counts[0]
    ):
        raise InjurySectionError("unreconciled_affected_clubs")
    # Even with two teams the captured cell publishes one total, after both team links.
    published = [{"club_id": club, "club_name": name} for club, name in clubs.items()]
    return _number(counts[0]), json.dumps(
        published, ensure_ascii=False, separators=(",", ":")
    )


def _history_pager(soup, block, capture, number):
    if page_number(capture.endpoint, capture.url) != number:
        raise InjurySectionError("unexpected_history_page")
    pagers = block.select(".tm-pagination")
    relatives = [link.get("href") for link in soup.select("link[rel~=next]")]
    if len(relatives) > 1:
        raise InjurySectionError("ambiguous_history_next_page")

    def checked(href):
        if not isinstance(href, str) or not href:
            raise InjurySectionError("invalid_history_page_link")
        url = urljoin(capture.url, href)
        if _route(capture.endpoint, url)[1] != _route(capture.endpoint, capture.url)[1]:
            raise InjurySectionError("cross_player_history_link")
        return page_number(capture.endpoint, url), url

    if not pagers:
        if (
            number != 1
            or relatives
            or block.select_one(".pager a, .pagination, .pager li")
        ):
            raise InjurySectionError("history_pager_missing")
        return None, None, 1
    if len(pagers) != 1:
        raise InjurySectionError("ambiguous_history_pager")
    active = []
    links = {}
    higher = []
    next_links = set()
    last_pages = set()
    for li in pagers[0].find_all("li", recursive=False):
        anchors = li.find_all("a", href=True)
        if len(anchors) != 1:
            raise InjurySectionError("invalid_history_page_link")
        n, url = checked(anchors[0]["href"])
        if links.setdefault(n, url) != url:
            raise InjurySectionError("ambiguous_history_page_link")
        classes = li.get("class", [])
        if "tm-pagination__list-item--active" in classes:
            active.append(n)
        if "tm-pagination__list-item--icon-last-page" in classes:
            last_pages.add(n)
        if "tm-pagination__list-item--icon-next-page" in classes:
            next_links.add((n, url))
        if n > number:
            higher.append(n)
    if active != [number] or (number > 1 and number - 1 not in links):
        raise InjurySectionError("history_pager_position")
    if len(last_pages) > 1 or last_pages and min(last_pages) < max(links):
        raise InjurySectionError("contradictory_history_final_page")
    last = next(iter(last_pages), None)
    if relatives:
        next_links.add(checked(relatives[0]))
    if higher or next_links:
        following = links.get(number + 1)
        if not following or (next_links and next_links != {(number + 1, following)}):
            raise InjurySectionError("nonsequential_history_pagination")
        return following, last, max(links)
    return None, last, max(links)


def _history_total(soup):
    headings = [
        h for h in soup.select(".content-box-headline") if _text(h).lower() == "total"
    ]
    if not headings:
        return None
    headers, rows = _table(_block(soup, {"total"}))
    if headers != ("season", "days", "injuries", "games missed"):
        raise InjurySectionError("unrecognized_history_summary")
    counts = [_number(row.find_all("td", recursive=False)[2]) for row in rows]
    if None in counts:
        raise InjurySectionError("unrecognized_history_summary")
    return sum(counts)


def _html_history(capture, player, number):
    soup = BeautifulSoup(capture.text, "html.parser")
    _source_identity(capture, soup)
    block = _block(soup, {"injury history"})
    headers, rows = _table(block)
    if headers != ("season", "injury", "from", "until", "days", "games missed"):
        raise InjurySectionError("history_headers")
    following, last, visible = _history_pager(soup, block, capture, number)
    total = _history_total(soup)
    if (
        (following and len(rows) != 15)
        or len(rows) > 15
        or (number > 1 and not rows)
        or (
            following is None
            and len(rows) == 15
            and not block.select_one(".tm-pagination")
            and total != 15
        )
    ):
        raise InjurySectionError("history_page_size_or_terminal_unproven")
    records = []
    for row in rows:
        season, injury, start, end, days, missed = row.find_all("td", recursive=False)
        raw_injury, season_label = _text(injury), _text(season)
        if raw_injury.casefold() in _MISSING or not re.fullmatch(
            r"(?:[0-9]{2}|[0-9]{4})(?:/[0-9]{2})?", season_label
        ):
            raise InjurySectionError("invalid_history_episode")
        since = current._optional_date(start, "invalid_history_start")
        until = current._optional_date(end, "invalid_history_end")
        if since is None or (until is not None and until < since):
            raise InjurySectionError("invalid_history_dates")
        length = _number(days, "days?")
        if length is not None:
            if length < 1 or (
                until
                and length
                != (date.fromisoformat(until) - date.fromisoformat(since)).days + 1
            ):
                raise InjurySectionError("injury_duration_conflict")
        games, clubs = _games_and_clubs(missed)
        records.append(
            dict(
                player_id=player,
                episode_key=identity(player, raw_injury, since),
                injury=raw_injury,
                injury_id=None,
                category=None,
                season_id=None,
                season_label=season_label,
                from_date=since,
                until_date=until,
                days=length,
                duration_days=None,
                games_missed=games,
                affected_clubs_json=clubs,
                page_number=number,
                source_next_url=following,
            )
        )
    return records, following, total, last, visible


def next_page(capture, page_number):
    if capture.endpoint not in PAGINATED or capture.status_code != 200:
        raise InjurySectionError("history_page_unavailable")
    player = scope_from_url(capture.endpoint, capture.url)["player_id"]
    return _html_history(capture, player, page_number)[1]


def _competition(cell):
    if cell is None:
        return None, None
    refs = _links(cell, _COMP_LINK)
    if len(refs) > 1:
        raise InjurySectionError("ambiguous_competition")
    if refs:
        return next(iter(refs.items()))
    name = _text(cell)
    return None, name if name.casefold() not in _MISSING else None


def _absence_end(cell):
    raw = _text(cell)
    try:
        return current._optional_date(cell, "invalid_absence_end"), None
    except ValueError:
        # A term expressed in games is not an end date. Preserve the published condition.
        if re.fullmatch(
            r"(?:[0-9]+\s+(?:match(?:es)?|games?|weeks?|months?|days?)"
            r"|until (?:further notice|the end of the season))",
            raw,
            re.I,
        ):
            return None, raw
        raise


def _club_absences(soup, club):
    missing = _block(
        soup,
        {
            "suspensions and injuries",
            "suspensions and absences",
            "suspensions & injuries",
        },
    )
    risk = _block(soup, {"risk of suspension"})
    absences, risks = [], []
    injured = 0
    for block in (missing, risk):
        _without_pager(block)
        unknown = block.select(".grid-view > span.empty")
        if unknown:
            if (
                len(unknown) != 1
                or _text(unknown[0]).casefold().rstrip(".")
                != "no information available"
                or block.select_one("table.items, .keys > span")
            ):
                raise InjurySectionError("conflicting_club_availability")
            if block is missing:
                absences = None
            else:
                risks = None
            continue
        headers, rows = _table(block, group_rows=block is missing)
        fields = {
            "player/position": "player",
            "expected return": "end",
            "return expected": "end",
            "until": "end",
            "from": "since",
            "missed matches": "games",
            "matches missed": "games",
            "games missed": "games",
            "yellow cards": "cards",
            "cards/match": "cards_per_match",
            "matches": "appearances",
            "games": "appearances",
            "market value": "market_value",
        }
        headers = tuple(fields.get(k, k) for k in headers)
        required = (
            {"player", "reason", "since", "end", "games"}
            if block is missing
            else {"player", "cards"}
        )
        allowed = required | {
            "age",
            "competition",
            "position",
            "market_value",
            "appearances",
            "cards_per_match",
        }
        if not required <= set(headers) <= allowed or len(set(headers)) != len(headers):
            raise InjurySectionError("absence_table_headers")
        group = None
        group_has_rows = False
        players_in_order = []
        for row in rows:
            values = row.find_all("td", recursive=False)
            if len(values) == 1:
                if group is not None and not group_has_rows:
                    raise InjurySectionError("unfinished_absence_group")
                group_has_rows = False
                group = _text(values[0])
                if group.casefold() not in {"injuries", "suspensions", "absences"}:
                    raise InjurySectionError("unknown_absence_group")
                continue
            cells = dict(zip(headers, values, strict=True))
            players = _links(cells["player"], _PLAYER_LINK)
            if len(players) != 1:
                raise InjurySectionError("invalid_absence_player")
            player = next(iter(players))
            players_in_order.append(player)
            group_has_rows = True
            cid, name = _competition(cells.get("competition"))
            if block is risk:
                cards = _number(cells["cards"])
                if cards is None:
                    raise InjurySectionError("risk_missing_card_count")
                ratio = (
                    _text(cells["cards_per_match"])
                    if "cards_per_match" in cells
                    else None
                )
                if ratio in _MISSING:
                    ratio = None
                if ratio is not None and not re.fullmatch(
                    r"[0-9]+(?:[.,][0-9]+)?", ratio
                ):
                    raise InjurySectionError("invalid_card_rate")
                for key in ("cards", "appearances"):
                    if key in cells and any(
                        not re.fullmatch(
                            r"/[A-Za-z0-9_-]+/leistungsdaten/spieler/" + player + r"/?",
                            a["href"],
                        )
                        for a in cells[key].select("a[href]")
                    ):
                        raise InjurySectionError("risk_player_mismatch")
                risks.append(
                    dict(
                        club_id=club,
                        player_id=player,
                        competition_id=cid,
                        competition_name=name,
                        competition_key=identity(cid or name),
                        yellow_cards=cards,
                        cards_per_match_raw=ratio,
                        appearances=_number(cells["appearances"])
                        if "appearances" in cells
                        else None,
                    )
                )
                continue
            raw = _text(cells["reason"])
            if raw.casefold() in _MISSING:
                raise InjurySectionError("missing_absence_reason")
            known_group = group.casefold() if group else None
            kind = absence_kind(raw, source_group=known_group)
            if known_group == "injuries" and kind != "injury":
                raise InjurySectionError("contradictory_absence_reason")
            if kind == "injury":
                if group and group.casefold() == "suspensions":
                    raise InjurySectionError("contradictory_absence_reason")
                injured += 1
                continue
            if kind == "risk_of_suspension":
                raise InjurySectionError("risk_in_actual_absences_table")
            since = current._optional_date(cells["since"], "invalid_absence_since")
            until, condition = _absence_end(cells["end"])
            if since and until and until < since:
                raise InjurySectionError("invalid_absence_date_order")
            badges = cells["reason"].select("img")
            logo = None
            if badges:
                if len(badges) != 1:
                    raise InjurySectionError("ambiguous_absence_competition")
                logo = badges[0].get("src", "")
                badge_name = badges[0].get("title") or badges[0].get("alt")
                if (
                    not re.fullmatch(
                        r"https://img\.a\.transfermarkt\.technology/logo/(?:tiny|small)/[A-Za-z0-9_-]+\.png(?:\?lm=[0-9]+)?",
                        logo,
                    )
                    or not badge_name
                    or name
                    and name != badge_name
                ):
                    raise InjurySectionError("invalid_absence_competition_badge")
                name = badge_name
            absences.append(
                dict(
                    club_id=club,
                    player_id=player,
                    competition_id=cid,
                    competition_name=name,
                    competition_logo_url=logo,
                    source_group=group,
                    absence_kind=kind,
                    absence_identity=identity(since, raw, cid or name),
                    reason_raw=raw,
                    since=since,
                    until_date=until,
                    end_condition=condition,
                    games_missed=_number(cells["games"], "(?:matches|games?)"),
                )
            )
        if group is not None and not group_has_rows:
            raise InjurySectionError("unfinished_absence_group")
        keys = block.select(".grid-view > .keys > span")
        if keys and [_text(node) for node in keys] != players_in_order:
            raise InjurySectionError("club_grid_player_mismatch")
    return absences, risks, injured


def _unique(entity, records):
    columns = tuple(k for k in TABLES[entity].natural_key if k != "observed_at")
    by_key = {}
    for record in records:
        key = tuple(record[k] for k in columns)
        if by_key.setdefault(key, record) != record:
            raise InjurySectionError("conflicting_observation_key")
    return list(by_key.values())


def extract(captures, scope, *, extracted_at, universe_version):
    """Fail an incomplete walk before allowing any of its history pages to be written."""
    if not captures:
        raise InjurySectionError("missing_injury_sections_capture")
    capture = captures[0]
    endpoint = capture.endpoint
    entities = ENDPOINT_ENTITIES[endpoint]
    revision = (
        "current-injuries-section-v2" if endpoint == "current_injury" else _REVISION
    )

    def outcome(entity, source, status, reason, rows=(), proof=None):
        return make_outcome(
            entity,
            source,
            scope,
            status=status,
            reason=reason,
            records=rows,
            proof=proof,
            parser_revision=revision,
            extracted_at=extracted_at,
            universe_version=universe_version,
        )

    def complete(entity, source, records, reason, proof):
        rows = _unique(entity, records)
        if rows:
            return outcome(
                entity,
                source,
                "observed_present",
                f"{reason}:rows={len(records)}:duplicates={len(records) - len(rows)}",
                rows,
            )
        return outcome(entity, source, "observed_empty", reason, proof=proof)

    try:
        if endpoint not in PAGINATED and len(captures) != 1:
            raise InjurySectionError("unexpected_additional_capture")
        seen_captures = set()
        for capture in captures:
            if capture.endpoint != endpoint or capture.capture_id in seen_captures:
                raise InjurySectionError("invalid_capture_set")
            seen_captures.add(capture.capture_id)
            target = scope_from_url(
                endpoint,
                capture.url,
                edition_id=scope.edition_id or "",
                competition_id=scope.competition_id or "",
            )
            if any(getattr(scope, key) != value for key, value in target.items()):
                raise InjurySectionError("request_scope_mismatch")
            if capture.status_code != 200:
                return tuple(
                    outcome(
                        e,
                        capture,
                        "unavailable",
                        f"{endpoint}:http_{capture.status_code}",
                    )
                    for e in entities
                )
        capture = captures[0]
        if endpoint in JSON_ENDPOINTS:
            collection = (
                "injuries" if endpoint == "player_injury_history" else "absences"
            )
            rows = api_history(capture.text, scope.player_id, collection=collection)
            return (
                complete(
                    entities[0],
                    capture,
                    rows,
                    endpoint,
                    f"player_matched_success:{collection}=[]",
                ),
            )
        if endpoint == "player_injury_history_html":
            pages, seen_episodes, totals = [], set(), set()
            for i, capture in enumerate(captures, 1):
                rows, following, total, last, visible = _html_history(
                    capture, scope.player_id, i
                )
                if visible > len(captures) or (
                    last is not None and last != len(captures)
                ):
                    raise InjurySectionError("history_announced_pages_missing")
                expected = captures[i].url if i < len(captures) else None
                if following != expected:
                    raise InjurySectionError("history_page_walk_incomplete")
                if total is not None:
                    totals.add(total)
                for row in rows:
                    if row["episode_key"] in seen_episodes:
                        raise InjurySectionError("history_page_episode_overlap")
                    seen_episodes.add(row["episode_key"])
                pages.append((capture, rows))
            if totals and totals != {len(seen_episodes)}:
                raise InjurySectionError("history_totals_mismatch")
            return tuple(
                complete(
                    _HISTORY,
                    page,
                    rows,
                    f"injury_history:page_{i}_of_{len(pages)}",
                    "identity_matched_injury_history:explicit_empty_table",
                )
                for i, (page, rows) in enumerate(pages, 1)
            )
        soup = BeautifulSoup(capture.text, "html.parser")
        _source_identity(capture, soup, scope.edition_id)
        if endpoint == "current_injury":
            records = _current_records(soup, scope)
            return (
                complete(
                    _CURRENT,
                    capture,
                    records,
                    "current_injury",
                    "edition_matched_injured_players:explicit_empty_table",
                ),
            )
        absences, risks, injured = _club_absences(soup, scope.club_id)
        absence_count = len(absences) if absences is not None else "unknown"
        risk_count = len(risks) if risks is not None else "unknown"
        reason = f"club_absences:injuries={injured}:absences={absence_count}:risks={risk_count}"
        return tuple(
            outcome(entity, capture, "unknown", "club_grid:no_information_available")
            if records is None
            else complete(
                entity,
                capture,
                records,
                reason,
                "club_matched_complete_table:" + reason,
            )
            for entity, records in ((_ABSENCES, absences), (_RISKS, risks))
        )
    except (InjurySectionError, current.CurrentInjuryParseError) as exc:
        reason = str(exc)
    except (ValueError, KeyError, TypeError, OverflowError, RecursionError):
        reason = "invalid_source"
    return tuple(outcome(e, capture, "error", f"{endpoint}:{reason}") for e in entities)
