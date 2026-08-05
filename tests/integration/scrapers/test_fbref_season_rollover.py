"""Real PostgreSQL semantics for an FBref season rollover.

FBref keeps the running season on the bare competition URL and moves the
outgoing season onto its dated URL.  Both the season registry and the page
frontier hold that URL under a unique constraint, so the handover has to be
released before the new edition claims it.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from scrapers.fbref.control import (
    CompetitionRegistryEntry,
    ControlStore,
    FrontierTarget,
    SeasonRegistryEntry,
    StateConflict,
)

# The disposable migrated database is shared with the admission suite.
from tests.integration.scrapers.test_fbref_control_admission import (  # noqa: F401
    isolated_postgres_uri,
)


pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _require_an_explicit_test_server():
    """Never let the inherited fixture create databases on the Airflow metabase."""
    if not os.getenv("FBREF_TEST_POSTGRES_URI", "").strip():
        pytest.skip("FBREF_TEST_POSTGRES_URI must be set explicitly")


COMPETITION = "12"
BARE_URL = "https://fbref.com/en/comps/12/La-Liga-Stats"
DATED_2025 = "https://fbref.com/en/comps/12/2025-2026/2025-2026-La-Liga-Stats"
DATED_2026 = "https://fbref.com/en/comps/12/2026-2027/2026-2027-La-Liga-Stats"
T0 = datetime(2026, 7, 1, 6, 0, tzinfo=timezone.utc)


def _seed_competition(store: ControlStore, fetched_at: datetime) -> None:
    snapshot = store.create_registry_snapshot(
        fetched_at=fetched_at,
        successful=True,
    )
    store.reconcile_competitions(
        snapshot,
        [
            CompetitionRegistryEntry(
                competition_id=COMPETITION,
                canonical_url=BARE_URL,
                name="La Liga",
                gender="male",
                classification="domestic_league",
            )
        ],
    )


def _reconcile(
    store: ControlStore,
    entries: list[SeasonRegistryEntry],
    fetched_at: datetime,
) -> dict:
    snapshot = store.create_registry_snapshot(
        fetched_at=fetched_at,
        successful=True,
    )
    return store.reconcile_seasons(snapshot, COMPETITION, entries)


def _season(season_id: str, url: str, *, current: bool = False):
    return SeasonRegistryEntry(
        competition_id=COMPETITION,
        season_id=season_id,
        canonical_url=url,
        label=season_id,
        is_current=current,
    )


def _query(uri: str, sql: str, params=None) -> list[dict]:
    import psycopg2

    connection = psycopg2.connect(uri, application_name="fbref-rollover-test")
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            columns = [column.name for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        connection.close()


def _registry(uri: str) -> dict[str, dict]:
    return {
        str(row["season_id"]): row
        for row in _query(
            uri,
            """
            SELECT season_id, canonical_url, is_current, present,
                   lifecycle_state
            FROM fbref_control.season_registry
            WHERE source = 'fbref' AND competition_id = %s
            """,
            (COMPETITION,),
        )
    }


def test_rollover_hands_the_bare_url_to_the_new_current_season(
    isolated_postgres_uri,  # noqa: F811
):
    """The outgoing season releases the bare URL it no longer owns."""
    pytest.importorskip("psycopg2")
    store = ControlStore(isolated_postgres_uri)
    _seed_competition(store, T0)
    _reconcile(
        store,
        [
            _season("2025-2026", BARE_URL, current=True),
            _season("2024-2025", DATED_2025.replace("2025-2026", "2024-2025")),
        ],
        T0,
    )

    # The new edition is published first, exactly as FBref orders its history
    # table, so it claims the bare URL while the outgoing season still holds it.
    result = _reconcile(
        store,
        [
            _season("2026-2027", BARE_URL, current=True),
            _season("2025-2026", DATED_2025),
        ],
        T0 + timedelta(days=1),
    )

    assert result["current"] == 1
    registry = _registry(isolated_postgres_uri)
    assert registry["2026-2027"]["canonical_url"] == BARE_URL
    assert registry["2026-2027"]["is_current"] is True
    assert registry["2025-2026"]["canonical_url"] == DATED_2025
    assert registry["2025-2026"]["is_current"] is False
    assert not [
        row for row in registry.values() if "#superseded:" in row["canonical_url"]
    ]


def test_rollover_when_the_new_season_already_holds_its_dated_url(
    isolated_postgres_uri,  # noqa: F811
):
    """The claimant may already exist: its update collides just the same."""
    pytest.importorskip("psycopg2")
    store = ControlStore(isolated_postgres_uri)
    _seed_competition(store, T0)
    _reconcile(
        store,
        [
            _season("2025-2026", BARE_URL, current=True),
            _season("2026-2027", DATED_2026),
        ],
        T0,
    )

    _reconcile(
        store,
        [
            _season("2026-2027", BARE_URL, current=True),
            _season("2025-2026", DATED_2025),
        ],
        T0 + timedelta(days=1),
    )

    registry = _registry(isolated_postgres_uri)
    assert registry["2026-2027"]["canonical_url"] == BARE_URL
    assert registry["2026-2027"]["is_current"] is True
    assert registry["2025-2026"]["canonical_url"] == DATED_2025


def test_outgoing_season_absent_from_the_snapshot_is_parked_and_swept(
    isolated_postgres_uri,  # noqa: F811
):
    """A released URL never stays fetchable when its owner is not republished."""
    pytest.importorskip("psycopg2")
    store = ControlStore(isolated_postgres_uri)
    _seed_competition(store, T0)
    _reconcile(store, [_season("2025-2026", BARE_URL, current=True)], T0)

    _reconcile(
        store,
        [_season("2026-2027", BARE_URL, current=True)],
        T0 + timedelta(days=1),
    )

    registry = _registry(isolated_postgres_uri)
    assert registry["2026-2027"]["canonical_url"] == BARE_URL
    assert registry["2025-2026"]["canonical_url"].startswith(BARE_URL + "#superseded:")
    assert registry["2025-2026"]["present"] is False
    assert not [
        row
        for row in store.list_backfill_seasons(limit=25)
        if row["season_id"] == "2025-2026"
    ]


def test_repeated_handovers_never_collide_on_a_parked_url(
    isolated_postgres_uri,  # noqa: F811
):
    """One URL may change hands many times without ever colliding."""
    pytest.importorskip("psycopg2")
    store = ControlStore(isolated_postgres_uri)
    _seed_competition(store, T0)
    _reconcile(store, [_season("2024-2025", BARE_URL, current=True)], T0)
    # 2024-2025 vanishes from the history table, so its parked URL survives.
    _reconcile(
        store,
        [_season("2025-2026", BARE_URL, current=True)],
        T0 + timedelta(days=1),
    )

    _reconcile(
        store,
        [
            _season("2026-2027", BARE_URL, current=True),
            _season("2025-2026", DATED_2025),
        ],
        T0 + timedelta(days=2),
    )

    registry = _registry(isolated_postgres_uri)
    assert registry["2026-2027"]["canonical_url"] == BARE_URL
    assert registry["2025-2026"]["canonical_url"] == DATED_2025
    assert registry["2024-2025"]["canonical_url"] == (
        BARE_URL + f"#superseded:{COMPETITION}:2024-2025"
    )


def _exec(uri: str, sql: str, params=None) -> None:
    import psycopg2

    connection = psycopg2.connect(uri, application_name="fbref-rollover-test")
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
        connection.commit()
    finally:
        connection.close()


def _season_target(season_id: str, url: str) -> FrontierTarget:
    return FrontierTarget(
        target_id=f"fbref:season:{COMPETITION}:{season_id}",
        page_kind="season",
        canonical_url=url,
        source_ids={"competition_id": COMPETITION, "season_id": season_id},
        refresh_policy="daily",
    )


def _frontier(uri: str) -> dict[str, dict]:
    return {
        str(row["target_id"]): row
        for row in _query(
            uri,
            """
            SELECT target_id, canonical_url, state, next_fetch_at
            FROM fbref_control.page_frontier
            """,
        )
    }


def _rolled_over_registry(store: ControlStore) -> None:
    """Reconcile the registry the way discovery does before it seeds targets."""
    _seed_competition(store, T0)
    _reconcile(
        store,
        [
            _season("2026-2027", BARE_URL, current=True),
            _season("2025-2026", DATED_2025),
        ],
        T0,
    )


def test_rollover_moves_the_outgoing_frontier_target_onto_its_dated_url(
    isolated_postgres_uri,  # noqa: F811
):
    """Admission succeeds and the outgoing edition keeps a fetchable URL."""
    pytest.importorskip("psycopg2")
    store = ControlStore(isolated_postgres_uri)
    _seed_competition(store, T0)
    _reconcile(store, [_season("2025-2026", BARE_URL, current=True)], T0)
    # Discovery seeded this target before the rollover, exactly as in
    # production: it owns the bare URL when the next snapshot arrives.
    store.upsert_frontier_target(_season_target("2025-2026", BARE_URL))
    _reconcile(
        store,
        [
            _season("2026-2027", BARE_URL, current=True),
            _season("2025-2026", DATED_2025),
        ],
        T0 + timedelta(days=1),
    )

    store.upsert_frontier_target(_season_target("2026-2027", BARE_URL))

    frontier = _frontier(isolated_postgres_uri)
    outgoing = frontier[f"fbref:season:{COMPETITION}:2025-2026"]
    assert outgoing["canonical_url"] == DATED_2025
    # Reconciliation already took the outgoing edition out of the current scope
    # before discovery seeded the new one; the release keeps that decision.
    assert outgoing["state"] == "skipped"
    assert (
        frontier[f"fbref:season:{COMPETITION}:2026-2027"]["canonical_url"] == BARE_URL
    )
    assert not [
        row for row in frontier.values() if "#superseded:" in row["canonical_url"]
    ]


def test_a_stale_edition_cannot_take_the_url_of_the_current_season(
    isolated_postgres_uri,  # noqa: F811
):
    """Release follows the registry, so it never runs backwards."""
    pytest.importorskip("psycopg2")
    store = ControlStore(isolated_postgres_uri)
    _rolled_over_registry(store)
    store.upsert_frontier_target(_season_target("2026-2027", BARE_URL))

    with pytest.raises(StateConflict, match="Canonical URL already belongs"):
        store.upsert_frontier_target(_season_target("2025-2026", BARE_URL))

    frontier = _frontier(isolated_postgres_uri)
    assert (
        frontier[f"fbref:season:{COMPETITION}:2026-2027"]["canonical_url"] == BARE_URL
    )


def test_release_is_refused_when_the_registry_url_is_already_taken(
    isolated_postgres_uri,  # noqa: F811
):
    """Freeing one URL must never violate the constraint on another."""
    pytest.importorskip("psycopg2")
    store = ControlStore(isolated_postgres_uri)
    _rolled_over_registry(store)
    store.upsert_frontier_target(_season_target("2025-2026", BARE_URL))
    store.upsert_frontier_target(_season_target("2024-2025", DATED_2025))

    with pytest.raises(StateConflict, match="Canonical URL already belongs"):
        store.upsert_frontier_target(_season_target("2026-2027", BARE_URL))


def test_a_foreign_competition_still_blocks_a_canonical_url(
    isolated_postgres_uri,  # noqa: F811
):
    """Releasing needs registry evidence, which a foreign target never has."""
    pytest.importorskip("psycopg2")
    store = ControlStore(isolated_postgres_uri)
    store.upsert_frontier_target(
        FrontierTarget(
            target_id="fbref:season:99:2025-2026",
            page_kind="season",
            canonical_url=BARE_URL,
            source_ids={"competition_id": "99", "season_id": "2025-2026"},
            refresh_policy="daily",
        )
    )

    with pytest.raises(StateConflict, match="Canonical URL already belongs"):
        store.upsert_frontier_target(_season_target("2026-2027", BARE_URL))


def test_a_parked_registry_url_never_reaches_the_frontier(
    isolated_postgres_uri,  # noqa: F811
):
    """A released target must never inherit a parked, unfetchable URL."""
    pytest.importorskip("psycopg2")
    store = ControlStore(isolated_postgres_uri)
    _seed_competition(store, T0)
    _reconcile(store, [_season("2025-2026", BARE_URL, current=True)], T0)
    store.upsert_frontier_target(_season_target("2025-2026", BARE_URL))
    # The outgoing season drops out of the history table, so the registry parks
    # it on a sentinel instead of a dated URL.
    _reconcile(
        store,
        [_season("2026-2027", BARE_URL, current=True)],
        T0 + timedelta(days=1),
    )

    with pytest.raises(StateConflict, match="Canonical URL already belongs"):
        store.upsert_frontier_target(_season_target("2026-2027", BARE_URL))

    frontier = _frontier(isolated_postgres_uri)
    assert not [
        row for row in frontier.values() if "#superseded:" in row["canonical_url"]
    ]


def test_release_drops_the_cache_validators_of_the_url_it_leaves(
    isolated_postgres_uri,  # noqa: F811
):
    """A moved target points at another resource, so its 304 evidence dies."""
    pytest.importorskip("psycopg2")
    store = ControlStore(isolated_postgres_uri)
    _rolled_over_registry(store)
    store.upsert_frontier_target(_season_target("2025-2026", BARE_URL))
    _exec(
        isolated_postgres_uri,
        """
        UPDATE fbref_control.page_frontier
        SET last_etag = 'W/"bare"',
            last_modified = 'Tue, 04 Aug 2026 06:00:00 GMT',
            last_content_hash = %s
        WHERE target_id = %s
        """,
        ("a" * 64, f"fbref:season:{COMPETITION}:2025-2026"),
    )

    store.upsert_frontier_target(_season_target("2026-2027", BARE_URL))

    validators = _query(
        isolated_postgres_uri,
        """
        SELECT canonical_url, last_etag, last_modified, last_content_hash
        FROM fbref_control.page_frontier WHERE target_id = %s
        """,
        (f"fbref:season:{COMPETITION}:2025-2026",),
    )[0]
    assert validators["canonical_url"] == DATED_2025
    assert validators["last_etag"] is None
    assert validators["last_modified"] is None
    # The content fence of the observation already in flight survives the move.
    assert validators["last_content_hash"] == "a" * 64


def test_a_calendar_change_hands_the_bare_url_across_season_id_shapes(
    isolated_postgres_uri,  # noqa: F811
):
    """Annual and cross-year ids coexist in one competition (comp 49 today)."""
    pytest.importorskip("psycopg2")
    store = ControlStore(isolated_postgres_uri)
    _seed_competition(store, T0)
    _reconcile(
        store,
        [
            _season("2025", BARE_URL, current=True),
            _season("2024", DATED_2025.replace("2025-2026/2025-2026", "2024/2024")),
        ],
        T0,
    )

    _reconcile(
        store,
        [
            _season("2026-2027", BARE_URL, current=True),
            _season("2026", DATED_2026.replace("2026-2027/2026-2027", "2026/2026")),
            _season("2025", DATED_2025.replace("2025-2026/2025-2026", "2025/2025")),
        ],
        T0 + timedelta(days=1),
    )

    registry = _registry(isolated_postgres_uri)
    assert registry["2026-2027"]["canonical_url"] == BARE_URL
    assert registry["2026-2027"]["is_current"] is True
    assert "#superseded:" not in registry["2025"]["canonical_url"]
    assert len([row for row in registry.values() if row["is_current"]]) == 1


def test_a_parked_season_is_never_minted_into_a_backfill_cohort(
    isolated_postgres_uri,  # noqa: F811
):
    """Canonicalisation drops the fragment, so a sentinel must never be seeded."""
    pytest.importorskip("psycopg2")
    store = ControlStore(isolated_postgres_uri)
    _seed_competition(store, T0)
    _reconcile(
        store,
        [
            _season("2026-2027", BARE_URL, current=True),
            _season("2025-2026", DATED_2025),
        ],
        T0,
    )
    assert [
        row["season_id"] for row in store.list_backfill_seasons(limit=25)
    ] == ["2025-2026"]

    # A row left published on a sentinel is the dangerous case: the fragment
    # would be stripped and the cohort would fetch whoever owns the URL now.
    _exec(
        isolated_postgres_uri,
        """
        UPDATE fbref_control.season_registry
        SET canonical_url = canonical_url || '#superseded:12:2025-2026'
        WHERE competition_id = %s AND season_id = '2025-2026'
        """,
        (COMPETITION,),
    )

    assert store.list_backfill_seasons(limit=25) == []


def test_a_cross_competition_handover_fails_loudly(
    isolated_postgres_uri,  # noqa: F811
):
    """Releasing stops at the competition on purpose, so this must not pass."""
    psycopg2 = pytest.importorskip("psycopg2")
    shared = "https://fbref.com/en/matches/deadbeef"
    store = ControlStore(isolated_postgres_uri)
    snapshot = store.create_registry_snapshot(fetched_at=T0, successful=True)
    store.reconcile_competitions(
        snapshot,
        [
            CompetitionRegistryEntry(
                competition_id=competition_id,
                canonical_url=f"https://fbref.com/en/comps/{competition_id}/X",
                name=f"Competition {competition_id}",
                gender="male",
                classification="domestic_league",
            )
            for competition_id in (COMPETITION, "99")
        ],
    )
    store.reconcile_seasons(
        store.create_registry_snapshot(fetched_at=T0, successful=True),
        "99",
        [
            SeasonRegistryEntry(
                competition_id="99",
                season_id="2025",
                canonical_url=shared,
                metadata={"direct_match_only": "true"},
            )
        ],
    )

    # Only the sweep can retire a parked row and it is scoped to one
    # competition, so a silent cross-competition release would leave a
    # published sentinel that canonicalises straight back onto the live URL.
    with pytest.raises(psycopg2.errors.UniqueViolation):
        _reconcile(
            store,
            [_season("2025-2026", shared, current=True)],
            T0 + timedelta(days=1),
        )
