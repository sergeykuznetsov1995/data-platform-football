"""Editions cache between waves (#1504): recomputed from core, 24 h old at most."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from scrapers.espn import editions_store, urls
from scrapers.espn.denominator import load_denominator
from scrapers.espn.editions import EditionState
from scrapers.espn.transport_contracts import HttpStatusError
from tests.unit.scrapers.test_espn_probes import PROBES
from tests.unit.scrapers.test_espn_wave import FakeClient, _req_key

NOW = datetime(2026, 9, 25, 13, tzinfo=timezone.utc)
UCL = "uefa.champions"


def _rows(*slugs):
    denominator = load_denominator()
    return [denominator.row(slug) for slug in slugs]


def _client(**extra) -> FakeClient:
    responses = {
        _req_key(urls.league_detail(UCL)): (PROBES / "league_detail_uefa.champions.json").read_bytes()
    }
    responses.update(extra)
    return FakeClient(responses)


def _load(path, client, *, now=NOW, terminal=None):
    return editions_store.load_or_refresh(
        path,
        client=client,
        rows=_rows(UCL),
        schedule_terminal=lambda: terminal or {},
        now=now,
    )


def _old(year: int, *, open_: bool = True) -> EditionState:
    return EditionState(UCL, year, f"{year}-{year + 1 - 2000} UCL", date(year, 7, 1), date(year + 1, 6, 30), open=open_)


@pytest.mark.unit
def test_missing_file_is_computed_from_core_and_saved(tmp_path) -> None:
    path = tmp_path / "state" / "editions.json"
    client = _client()

    snapshot = _load(path, client)

    assert client.calls == [(_req_key(urls.league_detail(UCL)), True)]
    (state,) = snapshot.open_of(UCL)
    assert (state.year, state.display_name, state.start, state.open) == (
        2026, "2026-27 UEFA Champions League", date(2026, 7, 1), True
    )
    assert editions_store.load(path) == snapshot


@pytest.mark.unit
def test_fresh_file_is_used_and_a_day_old_one_is_recomputed(tmp_path) -> None:
    path = tmp_path / "editions.json"
    editions_store.save(path, editions_store.EditionsSnapshot(NOW - timedelta(hours=23), (_old(2026),)))
    client = _client()

    assert _load(path, client).open_of(UCL) == (_old(2026),)
    assert client.calls == []

    stale = _load(path, client, now=NOW + timedelta(hours=2))
    assert len(client.calls) == 1
    assert stale.refreshed_at == NOW + timedelta(hours=2)


@pytest.mark.unit
def test_unreadable_file_is_recomputed(tmp_path) -> None:
    path = tmp_path / "editions.json"
    path.write_text("{not json", encoding="utf-8")

    assert _load(path, _client()).open_of(UCL)[0].year == 2026


@pytest.mark.unit
def test_season_transition_opens_the_new_edition_and_closes_a_finished_one(tmp_path) -> None:
    path = tmp_path / "editions.json"
    old = editions_store.EditionsSnapshot(NOW - timedelta(days=2), (_old(2025),))
    editions_store.save(path, old)

    both = _load(path, _client())
    assert [state.year for state in both.open_of(UCL)] == [2026, 2025]

    editions_store.save(path, old)
    closed = _load(path, _client(), terminal={UCL: {2025: True}})
    assert [state.year for state in closed.open_of(UCL)] == [2026]
    assert closed.edition(UCL, 2025).open is False


@pytest.mark.unit
def test_failing_league_keeps_its_known_editions(tmp_path) -> None:
    path = tmp_path / "editions.json"
    editions_store.save(path, editions_store.EditionsSnapshot(NOW - timedelta(days=2), (_old(2025),)))
    client = _client(**{_req_key(urls.league_detail(UCL)): HttpStatusError(404, "gone")})

    snapshot = _load(path, client)

    assert snapshot.open_of(UCL) == (_old(2025),)
    assert snapshot.refreshed_at == NOW


@pytest.mark.unit
def test_default_path_sits_next_to_the_gate_state(monkeypatch) -> None:
    monkeypatch.delenv(editions_store.EDITIONS_STATE_ENV, raising=False)
    monkeypatch.setenv("AIRFLOW_HOME", "/opt/airflow")
    assert str(editions_store.default_state_path()) == "/opt/airflow/state/espn/editions.json"
    monkeypatch.setenv(editions_store.EDITIONS_STATE_ENV, "/tmp/x.json")
    assert str(editions_store.default_state_path()) == "/tmp/x.json"


@pytest.mark.unit
def test_single_403_keeps_known_editions_all_blocked_propagates(tmp_path) -> None:
    from scrapers.espn.transport_contracts import AllOriginsBlocked, OriginBlocked

    path = tmp_path / "editions.json"
    editions_store.save(path, editions_store.EditionsSnapshot(NOW - timedelta(days=2), (_old(2025),)))
    key = _req_key(urls.league_detail(UCL))

    snapshot = _load(path, _client(**{key: OriginBlocked("403 core")}))
    assert snapshot.failed == {UCL: "OriginBlocked: 403 core"}
    assert snapshot.open_of(UCL) == (_old(2025),)

    editions_store.save(path, editions_store.EditionsSnapshot(NOW - timedelta(days=2), (_old(2025),)))
    with pytest.raises(AllOriginsBlocked):
        _load(path, _client(**{key: AllOriginsBlocked("every origin 403")}))
