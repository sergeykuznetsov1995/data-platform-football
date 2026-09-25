"""ESPN request addresses of the new contour (#1501): core lists, one-day scoreboards."""

from __future__ import annotations

from datetime import date
import inspect
from pathlib import Path
import re
from urllib.parse import parse_qsl, urlsplit

import pytest

from scrapers.espn import urls
from scrapers.espn.transport_contracts import EndpointType

README = (
    Path(__file__).resolve().parents[2] / "fixtures" / "espn" / "probes" / "README.md"
)


def _readme_urls() -> dict[str, str]:
    found = {}
    for line in README.read_text(encoding="utf-8").splitlines():
        match = re.match(r"\| `([^`]+)` \|.*\| (https://\S+) \|$", line)
        if match:
            found[match.group(1)] = match.group(2)
    return found


def _split(url: str) -> tuple[str, dict[str, str]]:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}{parts.path}", dict(parse_qsl(parts.query))


@pytest.mark.unit
@pytest.mark.parametrize(
    ("fixture", "request_"),
    [
        ("league_detail_uefa.champions.json", urls.league_detail("uefa.champions")),
        (
            "seasons_uefa.champions.json",
            urls.league_seasons("uefa.champions", limit=100),
        ),
        ("seasons_eng.fa_page0_limit3.json", urls.league_seasons("eng.fa", limit=3)),
        ("season_uefa.champions_2010.json", urls.season("uefa.champions", 2010)),
        ("types_uefa.champions_2026.json", urls.season_types("uefa.champions", 2026)),
        ("types_eng.fa_2026_empty.json", urls.season_types("eng.fa", 2026)),
        (
            "type_events_uefa.champions_2026_t1.json",
            urls.type_events("uefa.champions", 2026, 1),
        ),
        (
            "type_events_fifa.world_2010_t1.json",
            urls.type_events("fifa.world", 2010, 1),
        ),
        (
            "core_events_ucl_2010_type5.json",
            urls.type_events("uefa.champions", 2010, 5),
        ),
        (
            "events_window_eng1_30d.json",
            urls.events_window("eng.1", date(2026, 9, 1), date(2026, 9, 30)),
        ),
        (
            "event_status_first_half.json",
            urls.event_status("uefa.nations", 401861047),
        ),
        (
            "all_scoreboard_20260923.json",
            urls.all_scoreboard_day(date(2026, 9, 23)),
        ),
        (
            "scoreboard_eng1_20050813_postponed.json",
            urls.league_scoreboard_day("eng.1", date(2005, 8, 13)),
        ),
        (
            "scoreboard_eng1_day.json",
            urls.league_scoreboard_day("eng.1", date(2026, 9, 20)),
        ),
    ],
)
def test_addresses_match_the_recorded_probe_urls(fixture, request_) -> None:
    recorded = _readme_urls()[fixture]
    assert _split(request_.full_url) == _split(recorded)


@pytest.mark.unit
def test_core_goes_to_core_and_scoreboard_to_web_api() -> None:
    assert urls.league_detail("eng.1").url.startswith(
        "https://sports.core.api.espn.com/v2/sports/soccer/leagues/"
    )
    assert urls.all_scoreboard_day(date(2026, 9, 23)).url == (
        "https://site.web.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard"
    )
    assert urls.type_events("eng.1", 2025, 1).endpoint is EndpointType.SCOREBOARD
    assert urls.season_types("eng.1", 2025).endpoint is EndpointType.CATALOG


@pytest.mark.unit
def test_pages_after_the_first_carry_page_number() -> None:
    assert "page" not in urls.league_seasons("eng.fa", limit=3).params
    assert urls.league_seasons("eng.fa", 9, limit=3).params["page"] == "9"
    with pytest.raises(ValueError, match="page"):
        urls.league_seasons("eng.fa", 0)


@pytest.mark.unit
def test_core_window_is_capped_at_365_days() -> None:
    # 20260101-20261231 (365 days) answered 200; 20250601-20260701 answered 400.
    accepted = urls.events_window("eng.1", date(2026, 1, 1), date(2026, 12, 31))
    assert accepted.params["dates"] == "20260101-20261231"
    with pytest.raises(ValueError, match="365"):
        urls.events_window("eng.1", date(2025, 6, 1), date(2026, 7, 1))
    with pytest.raises(ValueError, match="after"):
        urls.events_window("eng.1", date(2026, 2, 1), date(2026, 1, 1))
    one_day = urls.events_window("eng.1", date(2026, 9, 23), date(2026, 9, 23))
    assert one_day.params["dates"] == "20260923"


@pytest.mark.unit
def test_scoreboard_takes_exactly_one_day_never_a_range() -> None:
    # Criterion "0 range requests to scoreboard": the only scoreboard builders
    # take one ``date``; nothing in the module can emit dates=A-B to scoreboard.
    builders = [
        getattr(urls, name)
        for name in urls.__all__
        if callable(getattr(urls, name))
        and "scoreboard" in inspect.getsource(getattr(urls, name))
    ]
    assert {builder.__name__ for builder in builders} == {
        "league_scoreboard_day",
        "all_scoreboard_day",
    }
    for builder in builders:
        dated = [
            parameter
            for parameter in inspect.signature(builder).parameters.values()
            if parameter.annotation in ("date", date)
        ]
        assert [parameter.name for parameter in dated] == ["day"]
    request = urls.league_scoreboard_day("esp.1", date(2026, 9, 20))
    assert request.params["dates"] == "20260920"
    assert "-" not in request.params["dates"]
    with pytest.raises(TypeError):
        urls.league_scoreboard_day("esp.1", "20260801-20260831")  # type: ignore[arg-type]
    source = inspect.getsource(urls)
    assert source.count('"dates"') == 2  # events_window and league_scoreboard_day
