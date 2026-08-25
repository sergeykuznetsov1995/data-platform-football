"""CLI of the refresh lane's season-page sweep (lane F, issue #1218)."""

from __future__ import annotations

import json
from contextlib import contextmanager

import pytest

from dags.scripts import run_sofascore_schedule_refresh as refresh


SNAPSHOT = {
    "campaign_id": "c",
    "snapshot_id": "s" * 64,
    "tournaments": [
        {
            "capture_key": "SS-7",
            "unique_tournament_id": 7,
            "metadata_status": "ready",
            "seasons": [
                {"source_season_id": 96518, "start_year": 2026,
                 "canonical_season": "2627", "metadata_status": "pending"},
                {"source_season_id": 76953, "start_year": 2025,
                 "canonical_season": "2526", "metadata_status": "ready"},
            ],
        },
        {
            "capture_key": "SS-17",
            "unique_tournament_id": 17,
            "metadata_status": "ready",
            "seasons": [
                {"source_season_id": 76986, "start_year": 2025,
                 "canonical_season": "2526", "metadata_status": "ready"},
            ],
        },
        {
            "capture_key": "SS-23",
            "unique_tournament_id": 23,
            "metadata_status": "ready",
            "seasons": [
                {"source_season_id": 88001, "start_year": 2026,
                 "canonical_season": "2627", "metadata_status": "pending"},
            ],
        },
        {
            "capture_key": "SS-799",
            "unique_tournament_id": 799,
            "metadata_status": "pending",
            "seasons": [
                {"source_season_id": 76139, "start_year": 2025,
                 "canonical_season": "2526", "metadata_status": "pending"},
            ],
        },
    ],
}


def _target(tournament_id, season_id, league, canonical):
    return refresh.SeasonTarget(
        tournament_id=tournament_id, season_id=season_id,
        league=league, canonical_season=canonical,
    )


class _FakeClient:
    created: list[dict] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.closed = False
        _FakeClient.created.append(kwargs)

    @property
    def stats(self):
        return {"requests": 4, "paid_proxy_bytes": 2_500_000}

    def close(self):
        self.closed = True


@pytest.fixture
def offline(monkeypatch, tmp_path):
    """No network, no gateway, no Trino: every collaborator is a stub."""

    _FakeClient.created.clear()
    for name in (
        "SOFASCORE_PROXY_CONTROL_URL", "SOFASCORE_REFRESH_MAX_DUE",
        "SOFASCORE_REFRESH_MAX_STALE", "SOFASCORE_REFRESH_MAX_SEED",
        "SOFASCORE_REFRESH_SEED_PAGES", "SOFASCORE_REFRESH_CHASE_PAGES",
        "SOFASCORE_REFRESH_WINDOW_HOURS",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(refresh, "LeaseBrowserSofaScoreClient", _FakeClient)
    monkeypatch.setattr(refresh, "_configured_tournament_ids", lambda: frozenset({17}))
    calls = {"fetch": [], "writes": []}

    def partitions(window_hours):
        calls["window_hours"] = window_hours
        # Bronze answers with the newest kick-off of each known partition.
        known = {
            partition: calls.get("newest", {}).get(partition, 1_000_000)
            for partition in calls.get("known", set())
        }
        # Every known partition is assumed to owe a page unless the test says
        # otherwise: that is the strict reading of the absence check.
        return (
            known, calls.get("due", set()), calls.get("played", set(known)),
        )

    def fetch(
        client, targets, raw_store, *, max_pages=1, start_pages=None,
        resume_anchors=None, chase_before=None, owed_pages=None,
        missing_fail_share=refresh.fetch_season_schedules,
    ):
        targets = list(targets)
        calls["fetch"].append({
            "targets": targets, "max_pages": max_pages,
            "start_pages": dict(start_pages or {}),
            "resume_anchors": dict(resume_anchors or {}),
            "chase_before": dict(chase_before or {}),
            "owed_pages": set(owed_pages or ()),
            "missing_fail_share": missing_fail_share,
        })
        # A season whose page broke the endpoint contract, exactly as the real
        # fetcher answers for one: its whole visit is rolled back — no event, no
        # page, no counter — and a RESUMED chain comes back in the third value
        # at the page it still owed (Sol r23: a stub that dressed this up as an
        # ordinary truncation, event and all, hid the defect).
        broken = {
            pair for pair in targets if pair in calls.get("broken", set())
        }
        served = [pair for pair in targets if pair not in broken]
        truncated = [
            (tournament, season, calls.get("truncate_at", 4),
             calls.get("anchor_at", 1_700_000))
            for tournament, season in served
            if (tournament, season) in calls.get("truncate", set())
        ] + [
            (tournament, season, (start_pages or {})[(tournament, season)],
             (resume_anchors or {}).get((tournament, season), 0))
            for tournament, season in broken
            if (start_pages or {}).get((tournament, season))
        ]
        return (
            [f"event-{tournament}-{season}" for tournament, season in served],
            {"targets": len(targets), "pages": len(served),
             "events": len(served), "missing": 0, "truncated": len(truncated),
             "foreign_season": 0, "resumed": 0, "chased": 0,
             "chase_settled": 0, "malformed": len(broken),
             "malformed_resumed": len(broken)},
            truncated,
        )

    def fixtures(client, targets, raw_store):
        targets = list(targets)
        calls.setdefault("fixtures", []).append(targets)
        if set(targets) & set(calls.get("fixtures_fail", ())):
            raise refresh.ScheduleSweepError("calendar pages broke the contract")
        return (
            [f"fixture-{tournament}-{season}" for tournament, season in targets],
            {"targets": len(targets), "pages": len(targets),
             "events": len(targets), "missing": 0, "foreign_season": 0},
        )

    def rows(events, snapshot, exclude_leagues):
        events = list(events)
        calls.setdefault("rows", []).append((events, snapshot, exclude_leagues))
        return (
            [{"game_id": index, "league": "SS-7"} for index, _ in enumerate(events)],
            {"events": len(events), "matched": len(events), "excluded": 0,
             "unknown_seasons": 0},
        )

    def write(written):
        calls["writes"].append(written)
        return "bronze.sofascore_schedule"

    monkeypatch.setattr(refresh, "bronze_partitions", partitions)
    monkeypatch.setattr(refresh, "fetch_season_schedules", fetch)
    monkeypatch.setattr(refresh, "fetch_season_fixtures", fixtures)
    monkeypatch.setattr(refresh, "schedule_rows_from_events", rows)
    monkeypatch.setattr(refresh, "write_schedule_rows", write)
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(json.dumps(SNAPSHOT))
    return {
        "calls": calls,
        "snapshot": snapshot,
        "output": tmp_path / "schedule-refresh.json",
        "cursor": tmp_path / "cursor.json",
        "incomplete": tmp_path / "incomplete.json",
        "raw_store": f"file://{tmp_path / 'raw'}",
    }


def _argv(offline, *extra):
    return [
        "--snapshot", str(offline["snapshot"]),
        "--output", str(offline["output"]),
        "--cursor", str(offline["cursor"]),
        "--incomplete", str(offline["incomplete"]),
        "--raw-store-uri", offline["raw_store"],
        *extra,
    ]


def _digest(exclude=frozenset({17})):
    return refresh.targets_digest(refresh.current_season_targets(SNAPSHOT, exclude))


def _cursor_file(offline, index, *, targets=None):
    offline["cursor"].write_text(json.dumps({
        "snapshot_id": SNAPSHOT["snapshot_id"],
        "targets": _digest() if targets is None else targets,
        # Anchors, not indexes: [tournament_id, season_id] or null.
        "index": index,
    }))


def _fetch_by_class(offline):
    return dict(zip(refresh.SWEEP_CLASSES, offline["calls"]["fetch"]))


@pytest.mark.unit
def test_main_refreshes_due_seasons_and_seeds_unknown_ones(offline):
    # SS-7/2627 already has rows and plays now → tail page only.
    # SS-23/2627 has never been written → whole page chain.
    offline["calls"]["known"] = {("SS-7", "2627")}
    offline["calls"]["due"] = {("SS-7", "2627")}

    exit_code = refresh.main(_argv(
        offline,
        "--control-url", "http://sofascore-gw:8899",
        "--budget-cap-bytes", "44000000",
        "--seed-pages", "9",
        "--chase-pages", "2",
        "--run-id", "scheduled__2026-08-24T00:30:00+00:00",
    ))

    assert exit_code == 0
    calls = offline["calls"]
    by_class = _fetch_by_class(offline)
    assert by_class["due"]["targets"] == [(7, 96518)]
    assert by_class["due"]["max_pages"] == 2
    # A known partition carries the newest kick-off Bronze has, so the tail
    # visit can walk back to it instead of stopping at page 0.
    assert by_class["due"]["chase_before"] == {(7, 96518): 1_000_000}
    assert by_class["stale"]["targets"] == []
    assert by_class["seed"]["targets"] == [(23, 88001)]
    assert by_class["seed"]["max_pages"] == 9
    # A not-yet-started season legitimately has no page, and the seed slice is
    # full of them: absences there prove nothing and must not fail the run.
    assert by_class["seed"]["missing_fail_share"] is None
    assert by_class["seed"]["chase_before"] == {}
    assert calls["window_hours"] == 36
    # Only the classes that are not "playing right now" take the fixture page:
    # a due season is being refreshed for its result, not its calendar.
    assert calls["fixtures"] == [[], [(23, 88001)]]
    client = _FakeClient.created[0]
    assert client["control_url"] == "http://sofascore-gw:8899"
    assert client["budget_cap_bytes"] == 44_000_000
    assert client["dag_id"] == "dag_refresh_sofascore_all_mens"
    assert client["task_id"] == "refresh_season_schedules"
    assert calls["writes"] == [
        # due: the tail page.  seed: the page chain plus its fixture page.
        [{"game_id": 0, "league": "SS-7"}],
        [{"game_id": 0, "league": "SS-7"}, {"game_id": 1, "league": "SS-7"}],
    ]
    report = json.loads(offline["output"].read_text())
    assert report["status"] == "success"
    assert report["targets_total"] == 2
    assert report["due_targets"] == 1
    assert report["stale_targets"] == 0
    assert report["seed_targets"] == 1
    assert report["idle"] is False
    assert report["due"]["pages"] == 1
    assert report["stale"]["pages"] == 0
    assert report["seed"]["pages"] == 1
    assert report["seed_fixtures"]["pages"] == 1
    assert report["rows_written"] == 3
    assert report["table"] == "bronze.sofascore_schedule"
    assert report["discovery"]["paid_proxy_bytes"] == 2_500_000
    assert report["errors"] == []
    cursor = json.loads(offline["cursor"].read_text())
    assert cursor == {
        "snapshot_id": SNAPSHOT["snapshot_id"],
        "targets": _digest(),
        # This run wrote rows, so the emptiness alarm is back at zero.
        "idle_runs": 0,
        # Both classes have a single member, so each anchor stays where it is.
        "index": {"due": [7, 96518], "stale": [7, 96518], "seed": [7, 96518]},
    }
    # An anchor that stayed put means the class was walked whole — which is
    # only readable next to the size of the class itself.
    assert report["class_members"] == {"due": 1, "stale": 0, "seed": 1}
    assert report["idle_runs"] == 0


@pytest.mark.unit
def test_every_class_is_merged_before_the_next_one_is_fetched(offline, monkeypatch):
    # Sol r4 #4: a failure in the low-priority classes must not throw away the
    # due rows that were already paid for.
    snapshot = json.loads(offline["snapshot"].read_text())
    snapshot["tournaments"].append({
        "capture_key": "SS-31",
        "unique_tournament_id": 31,
        "metadata_status": "ready",
        "seasons": [
            {"source_season_id": 90001, "start_year": 2026,
             "canonical_season": "2627", "metadata_status": "pending"},
        ],
    })
    offline["snapshot"].write_text(json.dumps(snapshot))
    offline["calls"]["known"] = {("SS-7", "2627"), ("SS-31", "2627")}
    offline["calls"]["due"] = {("SS-7", "2627")}
    order: list[str] = []
    stub_fetch = refresh.fetch_season_schedules
    stub_write = refresh.write_schedule_rows

    def fetch(client, targets, raw_store, **kwargs):
        targets = list(targets)
        order.append(f"fetch:{len(targets)}")
        if targets == [(23, 88001)]:
            raise RuntimeError("gateway said 429")
        return stub_fetch(client, targets, raw_store, **kwargs)

    def write(rows):
        order.append("write")
        return stub_write(rows)

    monkeypatch.setattr(refresh, "fetch_season_schedules", fetch)
    monkeypatch.setattr(refresh, "write_schedule_rows", write)

    assert refresh.main(_argv(offline, "--control-url", "http://gw", "--max-due", "1")) == 1

    # due fetched and written, stale fetched and written, seed exploded — and the due rows
    # are in Bronze all the same.
    assert order == ["fetch:1", "write", "fetch:1", "write", "fetch:1"]
    assert offline["calls"]["writes"][0] == [{"game_id": 0, "league": "SS-7"}]
    report = json.loads(offline["output"].read_text())
    assert report["status"] == "failed"
    assert report["due_rows"] == 1
    # Sol r5 #1: the classes that finished commit their own cursor; a class
    # that keeps failing must not freeze the ones that work.
    saved = json.loads(offline["cursor"].read_text())["index"]
    assert saved["due"] == [23, 88001] and saved["seed"] is None


@pytest.mark.unit
def test_seed_slice_walks_its_own_cursor_over_unseeded_seasons(offline):
    _cursor_file(offline, {"due": None, "stale": None, "seed": [23, 88001]})

    assert refresh.main(_argv(
        offline, "--control-url", "http://gw", "--max-seed", "1",
    )) == 0

    assert _fetch_by_class(offline)["seed"]["targets"] == [(23, 88001)]
    # One member, cap of one: the anchor lands back on the target itself.
    assert json.loads(offline["cursor"].read_text())["index"]["seed"] == [7, 96518]


@pytest.mark.unit
def test_a_lone_due_season_does_not_drag_the_other_cursors(offline):
    # Sol r4 #1: with one shared cursor a single due season far down the
    # sequence pushed the cursor past everything else, and from then on the
    # sweep replayed one slice forever.  Each class walks its own queue.
    targets = [_target(n, n, f"SS-{n}", "2627") for n in range(10)]
    known = {target.partition for target in targets[:6]}
    due = {targets[5].partition}

    plan, cursors, members = refresh.plan_sweep(
        targets, known, due, {"due": None, "stale": None, "seed": None},
        max_due=10, max_stale=2, max_seed=2,
    )

    assert [target.pair for target in plan["due"]] == [(5, 5)]
    assert [target.pair for target in plan["stale"]] == [(0, 0), (1, 1)]
    assert [target.pair for target in plan["seed"]] == [(6, 6), (7, 7)]
    # The due class has fewer members than its cap, so its anchor stays at the
    # head; stale and seed stopped right behind their own slices.
    assert cursors == {"due": (0, 0), "stale": (2, 2), "seed": (8, 8)}

    plan, cursors, members = refresh.plan_sweep(
        targets, known, due, cursors, max_due=10, max_stale=2, max_seed=2,
    )

    assert [target.pair for target in plan["stale"]] == [(2, 2), (3, 3)]
    assert [target.pair for target in plan["seed"]] == [(8, 8), (9, 9)]


@pytest.mark.unit
def test_a_failed_run_leaves_the_cursor_where_it_was(offline, monkeypatch):
    _cursor_file(offline, {"due": 0, "stale": 0, "seed": 1})

    def fetch(client, targets, raw_store, **kwargs):
        raise RuntimeError("gateway said 429")

    monkeypatch.setattr(refresh, "fetch_season_schedules", fetch)

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 1

    assert json.loads(offline["cursor"].read_text())["index"]["seed"] == 1
    report = json.loads(offline["output"].read_text())
    assert report["status"] == "failed"
    assert report["errors"] == ["RuntimeError: gateway said 429"]
    assert report["discovery"]["requests"] == 4
    assert offline["calls"]["writes"] == []


@pytest.mark.unit
def test_the_work_of_a_finished_class_survives_a_failed_lease_close(
    offline, monkeypatch
):
    # A run is judged by the work it RECORDED (lesson #11): a lease that fails
    # to close makes the run red and leaves its byte accounting uncertain, but
    # repeating the slice does not make it certain — it just pays for the same
    # pages again, while the rows are in Bronze either way (Sol r25).
    class _RudeClient(_FakeClient):
        def close(self):
            raise RuntimeError("lease close failed")

    monkeypatch.setattr(refresh, "LeaseBrowserSofaScoreClient", _RudeClient)
    offline["calls"]["truncate"] = {(23, 88001)}

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 1

    report = json.loads(offline["output"].read_text())
    assert report["status"] == "failed"
    assert "lease close failed" in report["errors"][0]
    assert json.loads(offline["cursor"].read_text())["index"] == report["cursor_next"]
    assert report["cursor_next"]["stale"] is not None
    # ...and so is the chain the run cut short: its rows already reached
    # Bronze, which makes the partition "known", and losing the resume point
    # here would strand the rest of that season for good (cross-check).
    assert json.loads(offline["incomplete"].read_text())["seasons"] == [
        [23, 88001, 4, 1_700_000, 0]
    ]


@pytest.mark.unit
def test_a_stale_sequence_digest_does_not_reset_the_walk(offline):
    # Configuring a league into the daily ingest changes the sequence, but the
    # anchor is a source pair: the walk resumes at the first target behind it
    # instead of starting the round over.
    _cursor_file(
        offline,
        {"due": None, "stale": None, "seed": [23, 88001]},
        targets="a sequence that no longer matches",
    )

    assert refresh.main(_argv(
        offline, "--control-url", "http://gw", "--max-seed", "1",
    )) == 0

    assert _fetch_by_class(offline)["seed"]["targets"] == [(23, 88001)]


@pytest.mark.unit
def test_a_known_season_outside_the_window_is_still_revisited():
    # Sol r3 #1: ``schedule_last`` serves finished matches only, so a weekly
    # league leaves the [-36h, +6h] window days before its next round.  If the
    # only classes were "playing now" and "never seeded", that season would be
    # green-and-invisible forever and the lag would be unbounded.
    targets = [_target(n, n, f"SS-{n}", "2627") for n in range(1, 4)]
    known = {target.partition for target in targets}

    plan, cursors, members = refresh.plan_sweep(
        targets, known, due=set(), cursors={}, max_due=10, max_stale=2,
        max_seed=10,
    )

    assert plan["due"] == [] and plan["seed"] == []
    assert [target.pair for target in plan["stale"]] == [(1, 1), (2, 2)]
    # The cursor stops behind the slice, so the next run takes the third one.
    assert cursors["stale"] == (3, 3)


@pytest.mark.unit
def test_plan_sweep_splits_due_from_known_and_never_seeded():
    targets = [
        _target(7, 96518, "SS-7", "2627"),
        _target(23, 88001, "SS-23", "2627"),
        _target(31, 90001, "SS-31", "2627"),
    ]
    known = {("SS-7", "2627"), ("SS-31", "2627")}
    due = {("SS-7", "2627")}

    plan, _, _ = refresh.plan_sweep(
        targets, known, due, {}, max_due=10, max_stale=10, max_seed=10
    )

    assert [target.pair for target in plan["due"]] == [(7, 96518)]
    assert [target.pair for target in plan["stale"]] == [(31, 90001)]
    assert [target.pair for target in plan["seed"]] == [(23, 88001)]


@pytest.mark.unit
def test_a_capped_due_list_continues_where_its_cursor_stopped():
    # A busy day can have more due seasons than the cap: taking the same first
    # N every run would starve the tail for good.
    targets = [_target(n, n, f"SS-{n}", "2627") for n in range(1, 6)]
    due = {target.partition for target in targets}

    plan, cursors, members = refresh.plan_sweep(
        targets, known=set(), due=due, cursors={"due": (4, 4)}, max_due=2,
        max_stale=10, max_seed=10,
    )

    assert [target.pair for target in plan["due"]] == [(4, 4), (5, 5)]
    assert cursors["due"] == (1, 1)
    # Everything is due, so nothing is left for the other classes.
    assert plan["stale"] == [] and plan["seed"] == []


@pytest.mark.unit
def test_take_slice_wraps_around_the_sequence():
    targets = [_target(n, n, f"SS-{n}", "2627") for n in range(5)]

    picked, cursor = refresh.take_slice(
        targets, (3, 3), 3, lambda target: True
    )

    assert [target.pair for target in picked] == [(3, 3), (4, 4), (0, 0)]
    assert cursor == (1, 1)


@pytest.mark.unit
def test_take_slice_keeps_its_cursor_when_the_class_is_smaller_than_the_cap():
    targets = [_target(n, n, f"SS-{n}", "2627") for n in range(5)]

    picked, cursor = refresh.take_slice(
        targets, (2, 2), 3, lambda target: target.tournament_id == 4
    )

    assert [target.pair for target in picked] == [(4, 4)]
    assert cursor == (2, 2)


@pytest.mark.unit
def test_the_cursor_survives_a_target_that_disappeared():
    # The anchor is a source pair, so a target removed from the sequence (a
    # league configured into the daily ingest, a season rolled over) does not
    # send the walk back to the head — it resumes at the next target behind it.
    targets = [_target(n, n, f"SS-{n}", "2627") for n in (0, 1, 3, 4)]

    picked, cursor = refresh.take_slice(
        targets, (2, 2), 2, lambda target: True
    )

    assert [target.pair for target in picked] == [(3, 3), (4, 4)]
    assert cursor == (0, 0)


@pytest.mark.unit
def test_current_season_targets_take_the_newest_season_that_is_not_excluded():
    snapshot = {
        "tournaments": [
            {
                "capture_key": "SS-5",
                "unique_tournament_id": 5,
                "metadata_status": "ready",
                "seasons": [
                    {"source_season_id": 3, "start_year": 2026,
                     "canonical_season": "2627", "metadata_status": "excluded"},
                    {"source_season_id": 2, "start_year": 2025,
                     "canonical_season": "2526", "metadata_status": "ready"},
                ],
            },
            {
                "capture_key": "SS-6",
                "unique_tournament_id": 6,
                "metadata_status": "ready",
                "seasons": [
                    {"source_season_id": 9, "start_year": 2026,
                     "canonical_season": "2627", "metadata_status": "excluded"},
                ],
            },
        ]
    }

    targets = refresh.current_season_targets(snapshot, frozenset())

    assert [target.pair for target in targets] == [(5, 2)]
    assert targets[0].partition == ("SS-5", "2526")


@pytest.mark.unit
def test_current_season_targets_skip_configured_and_unready_tournaments():
    targets = refresh.current_season_targets(SNAPSHOT, frozenset({17}))

    assert [target.pair for target in targets] == [(7, 96518), (23, 88001)]


@pytest.mark.unit
def test_targets_digest_changes_with_the_sequence():
    first = refresh.current_season_targets(SNAPSHOT, frozenset({17}))
    second = refresh.current_season_targets(SNAPSHOT, frozenset())

    assert refresh.targets_digest(first) == refresh.targets_digest(list(first))
    assert refresh.targets_digest(first) != refresh.targets_digest(second)


@pytest.mark.unit
@pytest.mark.parametrize(
    "payload",
    [
        None,
        '{"index": 4}',
        '{"index": {"due": 4}}',
        '{"index": {"due": [7]}}',
        '{"index": {"due": [7, true]}}',
        "not json",
        "[]",
    ],
)
def test_a_cursor_that_is_not_an_anchor_starts_from_the_head(tmp_path, payload):
    path = tmp_path / "cursor.json"
    if payload is not None:
        path.write_text(payload)

    assert refresh.read_cursor(path) == {
        "due": None, "stale": None, "seed": None,
    }


@pytest.mark.unit
def test_the_cursor_outlives_a_reissued_snapshot(tmp_path):
    # The campaign rewrites its snapshot continuously, and an index-based cursor
    # had to be dropped every time — which reset ``stale`` to the head of the
    # sequence long before its ~7-run round completed, so the same first few
    # hundred seasons were refreshed forever and the rest never (cross-check).
    path = tmp_path / "cursor.json"
    path.write_text(json.dumps({
        "snapshot_id": "a totally different snapshot",
        "targets": "a totally different sequence",
        "index": {"due": [7, 96518], "stale": [23, 88001], "seed": None},
    }))

    assert refresh.read_cursor(path) == {
        "due": (7, 96518), "stale": (23, 88001), "seed": None,
    }


@pytest.mark.unit
def test_knobs_default_from_the_environment(offline, monkeypatch):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_URL", "http://env-gw:8899")
    monkeypatch.setenv("SOFASCORE_REFRESH_DISCOVERY_BUDGET_BYTES", "60000000")
    monkeypatch.setenv("SOFASCORE_REFRESH_MAX_SEED", "1")
    monkeypatch.setenv("SOFASCORE_REFRESH_SEED_PAGES", "4")
    monkeypatch.setenv("SOFASCORE_REFRESH_CHASE_PAGES", "5")
    monkeypatch.setenv("SOFASCORE_REFRESH_WINDOW_HOURS", "12")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "manual__env")

    assert refresh.main(_argv(offline)) == 0

    client = _FakeClient.created[0]
    assert client["control_url"] == "http://env-gw:8899"
    assert client["budget_cap_bytes"] == 60_000_000
    assert client["run_id"] == "manual__env"
    assert offline["calls"]["window_hours"] == 12
    by_class = _fetch_by_class(offline)
    assert by_class["seed"]["max_pages"] == 4
    assert len(by_class["seed"]["targets"]) == 1
    assert by_class["due"]["max_pages"] == 5


@pytest.mark.unit
@pytest.mark.parametrize(
    "flag", ["--max-due", "--max-stale", "--max-seed", "--seed-pages",
             "--chase-pages"]
)
def test_main_fails_closed_on_a_non_positive_bound(offline, flag):
    assert refresh.main(_argv(offline, "--control-url", "http://gw", flag, "0")) == 1

    report = json.loads(offline["output"].read_text())
    assert flag in report["errors"][0]
    assert _FakeClient.created == []


@pytest.mark.unit
def test_a_chain_cut_short_is_finished_by_the_next_run(offline):
    # The first pages already make the partition "known", so without the retry
    # list the rest of that season's chain would never be fetched again.
    offline["calls"]["truncate"] = {(23, 88001)}
    offline["calls"]["truncate_at"] = 9

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 0

    stored = json.loads(offline["incomplete"].read_text())
    assert stored == {"seasons": [[23, 88001, 9, 1_700_000, 0]], "cursor": None}
    report = json.loads(offline["output"].read_text())
    assert report["incomplete_seasons"] == 1

    # Second run: Bronze now knows both partitions, but the truncated season
    # comes back through the retry list — resuming at the page it stopped at,
    # not at page 0 — and drops off the list once the chain completes.
    offline["calls"]["known"] = {("SS-7", "2627"), ("SS-23", "2627")}
    offline["calls"]["truncate"] = set()
    offline["calls"]["fetch"].clear()

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 0

    seed_call = _fetch_by_class(offline)["seed"]
    assert seed_call["targets"] == [(23, 88001)]
    # The owed page as saved: the one-page overlap is the fetcher's business,
    # and doing it here made ``--seed-pages 1`` never advance.
    assert seed_call["start_pages"] == {(23, 88001): 9}
    assert json.loads(offline["incomplete"].read_text())["seasons"] == []
    assert json.loads(offline["output"].read_text())["resumed_targets"] == 1


@pytest.mark.unit
def test_an_unfinished_chain_is_kept_when_a_later_class_fails(offline, monkeypatch):
    # Sol r6 #1: the partition of a cut-short chain is already "known" (and can
    # be "due" as well), so the tail classes used to take it — walking from
    # page 0, not from where the chain stopped — and retire its resume point on
    # the way.  When the seed phase then failed, that tail was gone for good.
    offline["incomplete"].write_text(
        json.dumps({"seasons": [[23, 88001, 5, 1_700_000]]})
    )
    offline["calls"]["known"] = {("SS-7", "2627"), ("SS-23", "2627")}
    offline["calls"]["due"] = {("SS-23", "2627")}
    stub_fetch = refresh.fetch_season_schedules
    seeded: list[list[tuple[int, int]]] = []

    def fetch(client, targets, raw_store, **kwargs):
        targets = list(targets)
        if kwargs.get("start_pages"):
            seeded.append(targets)
            raise RuntimeError("gateway said 429")
        return stub_fetch(client, targets, raw_store, **kwargs)

    monkeypatch.setattr(refresh, "fetch_season_schedules", fetch)

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 1

    by_class = _fetch_by_class(offline)
    # The pinned season belongs to the seed phase alone, even though Bronze
    # calls its partition due.
    assert by_class["due"]["targets"] == []
    assert by_class["stale"]["targets"] == [(7, 96518)]
    assert seeded == [[(23, 88001)]]
    assert json.loads(offline["incomplete"].read_text())["seasons"] == [
        [23, 88001, 5, 1_700_000, 0]
    ]


@pytest.mark.unit
def test_the_fetcher_is_told_which_seasons_owe_a_page_in_source_ids(offline):
    # Sol r8 #1: Bronze speaks (league, canonical season), the source speaks
    # (tournament id, season id).  Handing the fetcher the Bronze keys made
    # every membership test false, and the absence check silently ran with an
    # empty denominator — the very hole round 7 was supposed to close.
    offline["calls"]["known"] = {("SS-7", "2627"), ("SS-23", "2627")}
    offline["calls"]["due"] = {("SS-7", "2627")}
    # Bronze holds a kicked-off match of SS-7 only.
    offline["calls"]["played"] = {("SS-7", "2627")}

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 0

    by_class = _fetch_by_class(offline)
    # Source ids, not Bronze keys — and SS-23, whose first match is still
    # ahead, owes nothing.  Both tail classes share the one set.
    assert by_class["due"]["owed_pages"] == {(7, 96518)}
    assert by_class["stale"]["owed_pages"] == {(7, 96518)}
    assert by_class["seed"]["owed_pages"] == set()


@pytest.mark.unit
def test_a_postponed_or_canceled_game_does_not_make_a_page_owed():
    # Sol r8 #2: those two statuses are terminally unplayed, and the capture
    # probe already excludes them.  A canceled fixture must not turn a legitimate
    # 404 into evidence of drift.
    sql = refresh.KNOWN_PARTITIONS_SQL.format(grace=refresh.PLAYED_GRACE_HOURS)

    assert "NOT IN ('postponed', 'canceled')" in sql
    assert f"- {refresh.PLAYED_GRACE_HOURS} * 3600" in sql
    # Sol r9 #3: a NULL kick-off counts as owed — only evidence buys a quiet
    # zero, exactly as the capture probe reads it.
    assert "start_timestamp IS NULL" in sql


@pytest.mark.unit
def test_bronze_partitions_splits_the_anchor_from_what_owes_a_page(monkeypatch):
    rows = {
        refresh.KNOWN_PARTITIONS_SQL.format(grace=refresh.PLAYED_GRACE_HOURS): [
            # finished match -> anchor and owes a page
            ("SS-7", "2627", 1_000_000, 3),
            # only a kicked-off fixture (or a NULL kick-off) -> owes a page
            # without an anchor to chase
            ("SS-23", "2627", None, 1),
            # nothing has kicked off yet -> neither
            ("SS-31", "2627", None, 0),
        ],
    }
    monkeypatch.setattr(
        refresh, "_trino_rows",
        lambda sql: rows.get(sql, [("SS-7", "2627")]),
    )

    known, due, played = refresh.bronze_partitions(36)

    assert known == {
        ("SS-7", "2627"): 1_000_000,
        ("SS-23", "2627"): None,
        ("SS-31", "2627"): None,
    }
    assert due == {("SS-7", "2627")}
    assert played == {("SS-7", "2627"), ("SS-23", "2627")}


@pytest.mark.unit
def test_the_known_partitions_query_parses_as_trino_and_says_what_it_means():
    # Sol r10 #3: substring checks do not prove the query runs.  Parsing it with
    # the Trino dialect does prove its shape, and the columns are asserted from
    # the parse tree rather than from the text.
    import sqlglot
    from sqlglot import exp

    sql = refresh.KNOWN_PARTITIONS_SQL.format(grace=refresh.PLAYED_GRACE_HOURS)
    tree = sqlglot.parse_one(sql, dialect="trino")

    assert [projection.alias_or_name for projection in tree.expressions] == [
        "league", "season", "newest", "owed",
    ]
    assert tree.args["group"] is not None
    assert isinstance(tree.find(exp.Table).this, exp.Identifier)
    assert tree.find(exp.Table).sql(dialect="trino") == (
        "iceberg.bronze.sofascore_schedule"
    )
    # Sol r11 #3: substring checks pass for the wrong query too — ``IN
    # ('postponed')`` instead of the exclusion, or ``<> 'finished'`` instead of
    # the equality.  Every clause below is asserted from the parse tree.
    owed = tree.expressions[3]
    counter = owed.this
    # ``owed`` is the conditional COUNT itself, not a column that happens to
    # mention the words.
    assert isinstance(counter, exp.CountIf)
    condition = counter.this
    assert isinstance(condition, exp.And)
    # Left: no kick-off time at all, or one that passed more than the grace ago.
    # A row without a time is a debt, not an excuse — fail-closed.
    kick_off = condition.this.unnest()
    assert isinstance(kick_off, exp.Or)
    assert isinstance(kick_off.this, exp.Is)
    assert kick_off.this.this.name == "start_timestamp"
    assert isinstance(kick_off.this.expression, exp.Null)
    assert isinstance(kick_off.expression, exp.LT)
    assert kick_off.expression.this.name == "start_timestamp"
    grace = kick_off.expression.expression
    assert isinstance(grace, exp.Sub) and isinstance(grace.this, exp.TimeToUnix)
    # ``to_unixtime`` of NOW, not of some column that happens to be a time.
    assert isinstance(grace.this.this, exp.CurrentTimestamp)
    # Hours times seconds — ``6 + 3600`` in brackets reads the same to a
    # literal-collecting assertion and means something else entirely
    # (Sol r12 #5).
    hours = grace.expression.unnest()
    assert isinstance(hours, exp.Mul)
    assert [int(side.name) for side in (hours.this, hours.expression)] == [
        refresh.PLAYED_GRACE_HOURS, 3600,
    ]
    assert not any(side.is_string for side in (hours.this, hours.expression))
    # Right: the unplayable statuses are EXCLUDED, and an absent status is not
    # one of them.
    excluded = condition.expression
    assert isinstance(excluded, exp.Not)
    membership = excluded.this
    assert isinstance(membership, exp.In)
    assert isinstance(membership.this, exp.Coalesce)
    assert membership.this.this.name == "status_type"
    # The fallback has to be a status that is NOT in the exclusion list, or a
    # row with no status at all would stop owing a page (Sol r12 #5).
    fallback = membership.this.expressions[0]
    assert isinstance(fallback, exp.Literal) and fallback.is_string
    assert fallback.name == "unknown"
    assert all(
        isinstance(literal, exp.Literal) and literal.is_string
        for literal in membership.expressions
    )
    assert sorted(literal.name for literal in membership.expressions) == [
        "canceled", "postponed",
    ]
    assert fallback.name not in {literal.name for literal in membership.expressions}
    # And the anchor is the newest FINISHED kick-off — an equality, so a
    # negated or widened comparison would not pass here.
    newest = tree.expressions[2].this
    assert isinstance(newest, exp.Max)
    branch = newest.this.args["ifs"][0]
    assert isinstance(branch.this, exp.EQ)
    assert branch.this.this.name == "status_type"
    # A bare identifier named ``finished`` has the same ``.name`` as the string
    # literal and compares a column to a column (Sol r12 #5).
    finished = branch.this.expression
    assert isinstance(finished, exp.Literal) and finished.is_string
    assert finished.name == "finished"
    assert isinstance(branch.args["true"], exp.Column)
    assert branch.args["true"].name == "start_timestamp"
    assert newest.this.args.get("default") is None


@pytest.mark.unit
def test_a_seed_slice_mute_on_both_endpoints_is_reported_not_fatal(
    offline, monkeypatch
):
    # Sol r11 #1: a season mute on both endpoints never produces a row, so its
    # partition never becomes ``known`` and it never leaves ``seed`` — the mute
    # ones accumulate until an all-mute slice is the normal state of the class.
    # Failing on it would arm a bomb that goes off on every later run, so the
    # muteness is reported (both ``missing`` counters) and the run stands.
    def fetch(client, targets, raw_store, **kwargs):
        targets = list(targets)
        return [], {"targets": len(targets), "pages": 0, "events": 0,
                    "missing": len(targets), "truncated": 0, "foreign_season": 0,
                    "resumed": 0, "chased": 0, "chase_settled": 0}, []

    monkeypatch.setattr(refresh, "fetch_season_schedules", fetch)
    monkeypatch.setattr(
        refresh, "fetch_season_fixtures",
        lambda client, targets, raw_store: (
            [], {"targets": len(list(targets)), "pages": 0, "events": 0,
                 "missing": len(list(targets)), "foreign_season": 0},
        ),
    )

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 0

    report = json.loads(offline["output"].read_text())
    assert report["status"] == "success" and report["rows_written"] == 0
    # The evidence an operator needs is in the report, target by target.
    assert report["seed"]["missing"] == report["seed"]["targets"] > 0
    assert (
        report["seed_fixtures"]["missing"] == report["seed_fixtures"]["targets"] > 0
    )
    # And the drift this used to guard against still fails the run where it
    # cannot be confused with a season that has not kicked off: the tail
    # classes, whose targets Bronze holds a kicked-off match of.
    assert offline["cursor"].exists()


@pytest.mark.unit
def test_the_retry_queue_rotates_even_when_its_head_keeps_failing(
    offline, monkeypatch
):
    # Sol r7 #3: the queue used to be sliced from the head every run, so one
    # entry that keeps failing hid everything behind it forever.
    snapshot = json.loads(offline["snapshot"].read_text())
    snapshot["tournaments"].append({
        "capture_key": "SS-31",
        "unique_tournament_id": 31,
        "metadata_status": "ready",
        "seasons": [
            {"source_season_id": 90001, "start_year": 2026,
             "canonical_season": "2627", "metadata_status": "pending"},
        ],
    })
    offline["snapshot"].write_text(json.dumps(snapshot))
    offline["calls"]["known"] = {
        ("SS-7", "2627"), ("SS-23", "2627"), ("SS-31", "2627"),
    }
    offline["incomplete"].write_text(json.dumps({"seasons": [
        [7, 96518, 3, 1_700_000],
        [23, 88001, 5, 1_700_000],
        [31, 90001, 7, 1_700_000],
    ]}))
    seeded: list[list[tuple[int, int]]] = []

    def fetch(client, targets, raw_store, **kwargs):
        targets = list(targets)
        if kwargs.get("start_pages"):
            seeded.append(targets)
            raise RuntimeError("gateway said 429")
        return [], {"targets": 0, "pages": 0, "events": 0, "missing": 0,
                    "truncated": 0, "foreign_season": 0, "resumed": 0,
                    "chased": 0, "chase_settled": 0}, []

    monkeypatch.setattr(refresh, "fetch_season_schedules", fetch)

    for _ in range(3):
        assert refresh.main(_argv(
            offline, "--control-url", "http://gw", "--max-seed", "1",
        )) == 1

    # Three runs, three different heads — and nothing dropped off the queue,
    # because none of them finished.
    assert seeded == [[(7, 96518)], [(23, 88001)], [(31, 90001)]]
    stored = json.loads(offline["incomplete"].read_text())
    assert len(stored["seasons"]) == 3 and stored["cursor"] == [7, 96518]


@pytest.mark.unit
def test_fixture_events_without_a_single_row_fail_the_run(offline, monkeypatch):
    # Sol r7 #4: "green but empty" had a hole on the fixture page.  A calendar
    # served full of another season's events produced no row, moved the cursor
    # on and passed — the tail-page guard never looked at it.
    offline["calls"]["known"] = {("SS-7", "2627"), ("SS-23", "2627")}

    def fetch(client, targets, raw_store, **kwargs):
        return [], {"targets": len(list(targets)), "pages": 0, "events": 0,
                    "missing": 1, "truncated": 0, "foreign_season": 0,
                    "resumed": 0, "chased": 0, "chase_settled": 0}, []

    monkeypatch.setattr(refresh, "fetch_season_schedules", fetch)
    monkeypatch.setattr(
        refresh, "fetch_season_fixtures",
        # The page came back with events, every one of another season.
        lambda *a: ([], {"targets": 1, "pages": 1, "events": 12, "missing": 0,
                         "foreign_season": 12}),
    )

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 1

    report = json.loads(offline["output"].read_text())
    assert "produced no schedule row" in report["errors"][0]
    assert report["stale_fixtures"]["foreign_season"] == 12


@pytest.mark.unit
def test_worst_case_pays_a_warm_up_for_every_lease():
    # Sol r7 #5: the warm-ups were a hardcoded guess.  The client re-mints its
    # lease at 90 % of the lease ceiling, so a small ceiling means many of them.
    knobs = (
        refresh.DEFAULT_MAX_DUE, refresh.DEFAULT_MAX_STALE,
        refresh.DEFAULT_MAX_SEED, refresh.DEFAULT_CHASE_PAGES,
        refresh.DEFAULT_SEED_PAGES,
    )

    assert (
        refresh.worst_case_bytes(*knobs, 1024 * 1024)
        > refresh.worst_case_bytes(*knobs, 8 * 1024 * 1024)
    )
    # Sol r8 #3: a response cannot be split across leases, so a lease with no
    # room for a page after its warm-up can never serve one — 100 KiB leaves
    # ~10 KiB against a measured 27 KiB page.
    with pytest.raises(ValueError, match="no room for a"):
        refresh.worst_case_bytes(*knobs, 100 * 1024)


@pytest.mark.unit
def test_the_default_plan_fits_the_byte_cap():
    # Sol r6 #5: the old estimate forgot the fixture page of every stale and
    # seeded season and the step-back allowance of a resumed chain.
    pages = refresh.worst_case_pages(
        refresh.DEFAULT_MAX_DUE, refresh.DEFAULT_MAX_STALE,
        refresh.DEFAULT_MAX_SEED, refresh.DEFAULT_CHASE_PAGES,
        refresh.DEFAULT_SEED_PAGES,
    )

    assert pages == 150 * 3 + 200 * 4 + 40 * 17
    assert refresh.worst_case_bytes(
        refresh.DEFAULT_MAX_DUE, refresh.DEFAULT_MAX_STALE,
        refresh.DEFAULT_MAX_SEED, refresh.DEFAULT_CHASE_PAGES,
        refresh.DEFAULT_SEED_PAGES,
    ) <= refresh.DEFAULT_BUDGET_CAP_BYTES


@pytest.mark.unit
def test_a_plan_too_big_for_the_cap_never_reaches_the_gateway(offline):
    # The knobs come from the environment: an override that cannot fit has to
    # fail before the first paid request, not halfway through the sweep.
    assert refresh.main(_argv(
        offline, "--control-url", "http://gw", "--max-stale", "4000",
    )) == 1

    report = json.loads(offline["output"].read_text())
    assert "byte cap" in report["errors"][0]
    assert _FakeClient.created == []


@pytest.mark.unit
def test_the_unfinished_chain_survives_a_new_campaign_snapshot(offline):
    # Sol r3 #3: the ids are the source's own.  Dropping the list whenever the
    # snapshot is reissued would lose the tail of every season that survived
    # into the new one — its partition is already "known", so nothing else
    # would ever ask for the rest of its pages.
    offline["incomplete"].write_text(
        json.dumps({"seasons": [[23, 88001, 5, 1_700_000]]})
    )
    snapshot = json.loads(offline["snapshot"].read_text())
    snapshot["snapshot_id"] = "n" * 64
    offline["snapshot"].write_text(json.dumps(snapshot))
    offline["calls"]["known"] = {("SS-7", "2627"), ("SS-23", "2627")}

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 0

    seed_call = _fetch_by_class(offline)["seed"]
    assert seed_call["targets"] == [(23, 88001)]
    assert seed_call["start_pages"] == {(23, 88001): 5}


@pytest.mark.unit
def test_a_queued_chain_that_never_moves_is_given_up_on(offline):
    # Sol r22 #1: a chain whose page breaks the endpoint contract now comes
    # back on the queue at the page it owed, because its earlier pages are in
    # Bronze and nothing else would ever ask for the rest.  A page that is
    # broken for good would then be paid for on every run for ever, and such
    # chains would pile up until they dominate a seed slice and fail it — the
    # self-arming shape of round 11.  Three fruitless visits, and the lane
    # drops the chain and says so in the report instead.
    offline["incomplete"].write_text(
        json.dumps({"seasons": [[23, 88001, 9, 1_700_000]]})
    )
    offline["calls"]["known"] = {("SS-7", "2627"), ("SS-23", "2627")}
    # Broken the way the source breaks it: the page fails the endpoint contract,
    # the visit is rolled back whole and the chain comes back at the very page
    # it owed — every run.
    offline["calls"]["broken"] = {(23, 88001)}

    for attempts in (1, 2):
        offline["calls"]["fetch"].clear()
        assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 0
        stored = json.loads(offline["incomplete"].read_text())
        assert stored["seasons"] == [[23, 88001, 9, 1_700_000, attempts]]
        assert json.loads(offline["output"].read_text())["abandoned_chains"] == []

    offline["calls"]["fetch"].clear()
    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 0

    assert json.loads(offline["incomplete"].read_text())["seasons"] == []
    # Not silently: the rest of that season stays unread, and the report is
    # what says so.
    assert json.loads(offline["output"].read_text())["abandoned_chains"] == [
        [23, 88001, 9, 1_700_000]
    ]


@pytest.mark.unit
def test_a_broken_calendar_fails_the_run_only_after_the_class_banks_its_work(
    offline,
):
    # Sol r24: the calendar walk is an extra on top of tail pages that are
    # already paid for.  Raising as soon as it failed threw those away AND left
    # the class before its cursor moved and before the resume queue was written
    # — so the next run rebuilt the same slice and broke on the same page, for
    # ever.  The run still fails; it just banks what it has first.
    offline["incomplete"].write_text(
        json.dumps({"seasons": [[23, 88001, 9, 1_700_000]]})
    )
    offline["calls"]["known"] = {("SS-7", "2627"), ("SS-23", "2627")}
    # The calendar breaks for the QUEUED season, so the failure lands on the
    # seed class — after the chain has come back from the fetcher (Sol r25: a
    # blanket failure hit the earlier ``stale`` class and never exercised this
    # path at all).  Its own page breaks too, which is what puts it back on the
    # queue at the page it owed.
    offline["calls"]["broken"] = {(23, 88001)}
    offline["calls"]["fixtures_fail"] = {(23, 88001)}

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 1

    report = json.loads(offline["output"].read_text())
    assert report["status"] == "failed"
    assert "ScheduleSweepError" in report["errors"][0]
    # The classes before it banked their rows...
    assert report["stale_rows"] > 0 and offline["calls"]["writes"]
    # ...the seed cursor moved, so the next run takes the NEXT slice...
    assert json.loads(offline["cursor"].read_text())["index"]["seed"] is not None
    # ...and the chain is queued again WITH its attempt counted, which is what
    # ages a permanently broken page out instead of paying for it for ever.
    assert json.loads(offline["incomplete"].read_text())["seasons"] == [
        [23, 88001, 9, 1_700_000, 1]
    ]


@pytest.mark.unit
def test_a_queued_chain_that_moves_forward_starts_its_count_over(offline):
    # The count is about a chain that does not MOVE, not about how long it has
    # been queued: a season longer than the page bound is resumed run after run
    # and must never be given up on while it is making progress.
    offline["incomplete"].write_text(
        json.dumps({"seasons": [[23, 88001, 9, 1_700_000, 2]]})
    )
    offline["calls"]["known"] = {("SS-7", "2627"), ("SS-23", "2627")}
    offline["calls"]["truncate"] = {(23, 88001)}
    offline["calls"]["truncate_at"] = 12

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 0

    stored = json.loads(offline["incomplete"].read_text())
    assert stored["seasons"] == [[23, 88001, 12, 1_700_000, 0]]
    assert json.loads(offline["output"].read_text())["abandoned_chains"] == []


@pytest.mark.unit
def test_the_queue_keeps_the_chains_this_run_did_not_visit():
    # A chain the retry slice did not reach keeps its entry AND its count; one
    # that was visited and did not come back is finished and drops off.
    seasons, abandoned = refresh.requeue_chains(
        {(1, 2): (5, 100, 2), (3, 4): (7, 200, 0)}, {(3, 4)}, (),
    )

    assert seasons == [[1, 2, 5, 100, 2]] and abandoned == []


@pytest.mark.unit
@pytest.mark.parametrize(
    "payload", ["not json", '{"seasons": "nope"}', '{"seasons": [[1, 2]]}',
                '{"seasons": [["a", "b", "c", "d"]]}',
                '{"seasons": [[1, 2, 0, 5]]}',
                '{"seasons": [[1, 2, "3", 5]]}',
                '{"seasons": [[1, 2, true, 5]]}',
                '{"seasons": [[1, 2, 3.5, 5]]}',
                # The attempt count is the fifth field and nothing beyond it.
                '{"seasons": [[1, 2, 3, 5, -1]]}',
                '{"seasons": [[1, 2, 3, 5, "1"]]}',
                '{"seasons": [[1, 2, 3, 5, 1, 9]]}'],
)
def test_an_unreadable_unfinished_list_fails_the_run(offline, payload):
    # Sol r4 #6: treating a corrupt file as "no unfinished chains" would let
    # this run overwrite it and lose those tails for good.
    offline["incomplete"].write_text(payload)

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 1

    report = json.loads(offline["output"].read_text())
    assert "incomplete.json" in report["errors"][0]
    assert _FakeClient.created == []
    assert offline["incomplete"].read_text() == payload


@pytest.mark.unit
def test_the_unfinished_chain_is_written_before_the_cursor(offline, monkeypatch):
    # Sol r3 #4: two renames are not one transaction.  A crash between them
    # must leave the sweep repeating a slice (MERGE makes that harmless), never
    # step over a chain whose tail nothing else would ask for.
    offline["calls"]["truncate"] = {(23, 88001)}
    written: list[str] = []
    real_atomic = refresh._atomic_json

    def atomic(path, value):
        written.append(path.name)
        if path.name == "cursor.json":
            raise OSError("disk went away")
        real_atomic(path, value)

    monkeypatch.setattr(refresh, "_atomic_json", atomic)

    with pytest.raises(OSError):
        refresh.main(_argv(offline, "--control-url", "http://gw"))

    # The unfinished list is written as each class finishes — before its rows
    # can make the partition "known" without a way back — and the cursor only
    # at the end.
    assert written[0] == "incomplete.json"
    assert written[-1] == "cursor.json"
    assert "cursor.json" not in written[:-1]
    assert json.loads(offline["incomplete"].read_text())["seasons"] == [
        [23, 88001, 4, 1_700_000, 0]
    ]
    assert not offline["cursor"].exists()


@pytest.mark.unit
def test_a_fully_seeded_campaign_keeps_revisiting_its_seasons(offline):
    # Everything is in Bronze and nothing is playing right now: the run is not
    # idle, it walks the cursor slice of known seasons.  This is the lane's
    # only guarantee that a match played tomorrow is ever noticed.
    offline["calls"]["known"] = {("SS-7", "2627"), ("SS-23", "2627")}
    offline["calls"]["due"] = set()

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 0

    stale_call = _fetch_by_class(offline)["stale"]
    assert stale_call["targets"] == [(7, 96518), (23, 88001)]
    assert stale_call["chase_before"] == {
        (7, 96518): 1_000_000, (23, 88001): 1_000_000,
    }
    report = json.loads(offline["output"].read_text())
    assert report["idle"] is False
    assert report["stale_targets"] == 2
    assert report["due_targets"] == 0 and report["seed_targets"] == 0
    assert report["status"] == "success"


@pytest.mark.unit
def test_an_empty_plan_over_a_non_empty_campaign_is_a_failure(offline, monkeypatch):
    # Every season is playing, known or new, so an empty plan means the split
    # is broken — and a green run that fetched nothing looks exactly like a
    # working one.
    monkeypatch.setattr(
        refresh, "plan_sweep",
        lambda *args, **kwargs: (
            {"due": [], "stale": [], "seed": []}, {},
            {"due": 0, "stale": 0, "seed": 0},
        ),
    )

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 1

    report = json.loads(offline["output"].read_text())
    assert report["idle"] is True
    assert "sweep plan is empty" in report["errors"][0]
    assert offline["calls"]["fetch"] == []


@pytest.mark.unit
def test_main_fails_closed_without_a_gateway_url(offline):
    exit_code = refresh.main(_argv(offline))

    assert exit_code == 1
    assert _FakeClient.created == []
    assert offline["calls"]["fetch"] == []
    report = json.loads(offline["output"].read_text())
    assert report["status"] == "failed"
    assert "control-url" in report["errors"][0]


@pytest.mark.unit
def test_main_fails_closed_when_the_snapshot_has_no_ready_season(offline):
    offline["snapshot"].write_text(json.dumps({"snapshot_id": "s", "tournaments": []}))

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 1

    report = json.loads(offline["output"].read_text())
    assert "no ready tournament season" in report["errors"][0]
    assert _FakeClient.created == []


@pytest.mark.unit
def test_report_reads_the_client_stats_after_close(offline, monkeypatch):
    # Sol r2 #4: the bytes of the current lease are billed to paid_proxy_bytes
    # only when the lease closes, so the report must read stats after close().
    class _LateBytesClient(_FakeClient):
        @property
        def stats(self):
            return {"requests": 4, "paid_proxy_bytes": 2_500_000 if self.closed else 0}

    monkeypatch.setattr(refresh, "LeaseBrowserSofaScoreClient", _LateBytesClient)

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 0
    report = json.loads(offline["output"].read_text())
    assert report["discovery"]["paid_proxy_bytes"] == 2_500_000


@pytest.mark.unit
def test_pages_that_produce_no_row_fail_the_run(offline, monkeypatch):
    # Sol r3 #5: the lane asks for the pages of its OWN targets, so pages
    # without a single row means the snapshot and the source drifted apart.
    # Green-with-zero-rows would keep the cursor moving over that drift.
    monkeypatch.setattr(
        refresh, "schedule_rows_from_events",
        lambda *a: ([], {"events": 5, "matched": 0, "excluded": 5,
                         "unknown_seasons": 0}),
    )

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 1
    assert offline["calls"]["writes"] == []
    report = json.loads(offline["output"].read_text())
    assert "rows_written" not in report
    assert "produced no schedule row" in report["errors"][0]
    # The class that hit the drift keeps its anchor, so the next run repeats
    # exactly that slice instead of stepping over it.
    assert json.loads(offline["cursor"].read_text())["index"]["seed"] is None


@pytest.mark.unit
def test_a_slice_of_seasons_that_have_not_started_is_not_a_failure(
    offline, monkeypatch
):
    # No page came back at all (every seed season answers 404 before kick-off):
    # zero rows is the honest outcome, and the cursor moves on.
    # The counters are what the real fetchers would return: ``targets`` counts
    # every target it was handed, and a slice of seasons before kick-off is
    # missing all of them (Sol r11 #1 — the old stub said ``targets=0,
    # missing=1``, a shape the fetcher cannot produce).
    def fetch(client, targets, raw_store, **kwargs):
        targets = list(targets)
        return [], {"targets": len(targets), "pages": 0, "events": 0,
                    "missing": len(targets), "truncated": 0, "foreign_season": 0,
                    "resumed": 0, "chased": 0, "chase_settled": 0}, []

    monkeypatch.setattr(refresh, "fetch_season_schedules", fetch)
    monkeypatch.setattr(
        refresh, "fetch_season_fixtures",
        lambda client, targets, raw_store: (
            [], {"targets": len(list(targets)), "pages": 0, "events": 0,
                 "missing": len(list(targets)), "foreign_season": 0},
        ),
    )

    assert refresh.main(_argv(offline, "--control-url", "http://gw")) == 0
    assert offline["calls"]["writes"] == []
    report = json.loads(offline["output"].read_text())
    assert report["rows_written"] == 0 and report["status"] == "success"
    assert offline["cursor"].exists()


@pytest.mark.unit
def test_write_schedule_rows_merges_by_game_inside_the_writer_lock(monkeypatch):
    events = []

    class _Scraper:
        def __init__(self, **kwargs):
            events.append(("scraper", kwargs))

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            events.append("scraper:exit")
            return False

        def _add_metadata(self, frame, entity_type):
            events.append(("metadata", entity_type, len(frame)))
            return frame

        def save_to_iceberg(self, **kwargs):
            events.append(("save", kwargs["table_name"], kwargs["natural_keys"],
                           kwargs["partition_cols"], len(kwargs["df"])))
            return "bronze.sofascore_schedule"

    @contextmanager
    def lock():
        events.append("lock:enter")
        yield True
        events.append("lock:exit")

    import scrapers.sofascore.scraper as scraper_module
    import scrapers.sofascore.writer_lock as writer_lock

    monkeypatch.setattr(scraper_module, "SofaScoreScraper", _Scraper)
    monkeypatch.setattr(writer_lock, "bronze_writer_lock", lock)

    table = refresh.write_schedule_rows([
        {"game_id": 1, "league": "SS-7", "season": "2627"},
        {"game_id": 2, "league": "SS-7", "season": "2627"},
    ])

    assert table == "bronze.sofascore_schedule"
    assert events == [
        ("scraper", {}),
        "lock:enter",
        ("metadata", "schedule", 2),
        ("save", "sofascore_schedule", ["league", "season", "game_id"],
         ["league", "season"], 2),
        "lock:exit",
        "scraper:exit",
    ]


def _mute_everything(monkeypatch):
    """Both endpoints answer 404 for every target — the lane writes nothing."""

    def fetch(client, targets, raw_store, **kwargs):
        targets = list(targets)
        return [], {"targets": len(targets), "pages": 0, "events": 0,
                    "missing": len(targets), "truncated": 0, "foreign_season": 0,
                    "resumed": 0, "chased": 0, "chase_settled": 0}, []

    monkeypatch.setattr(refresh, "fetch_season_schedules", fetch)
    monkeypatch.setattr(
        refresh, "fetch_season_fixtures",
        lambda client, targets, raw_store: (
            [], {"targets": len(list(targets)), "pages": 0, "events": 0,
                 "missing": len(list(targets)), "foreign_season": 0},
        ),
    )


@pytest.mark.unit
def test_a_lane_that_writes_nothing_for_too_long_fails(offline, monkeypatch):
    # Sol r12 #1: dropping the per-slice muteness check left a lane that can be
    # green and empty forever.  A per-slice rule cannot close that (mute seed
    # targets accumulate until an all-mute slice is normal), but the LANE
    # writing nothing at all for days is never normal.
    offline["calls"]["known"] = {("SS-7", "2627")}
    _mute_everything(monkeypatch)

    for run in range(1, 6):
        assert refresh.main(_argv(
            offline, "--control-url", "http://gw", "--max-idle-runs", "6",
        )) == 0
        report = json.loads(offline["output"].read_text())
        assert report["idle_runs"] == run
        assert json.loads(offline["cursor"].read_text())["idle_runs"] == run

    # The sixth empty run in a row is the alarm.
    assert refresh.main(_argv(
        offline, "--control-url", "http://gw", "--max-idle-runs", "6",
    )) == 1
    report = json.loads(offline["output"].read_text())
    assert "written no schedule row for 6 runs" in report["errors"][0]
    # The count is still saved, so the lane keeps complaining until it is fixed
    # — and the classes it did walk kept their cursors.
    assert json.loads(offline["cursor"].read_text())["idle_runs"] == 6


@pytest.mark.unit
def test_one_row_clears_the_emptiness_alarm(offline, monkeypatch):
    # The guard has to let go by itself: a source that comes back must not need
    # an operator to reset a counter.
    offline["calls"]["known"] = {("SS-7", "2627")}
    _mute_everything(monkeypatch)
    for _ in range(3):
        assert refresh.main(_argv(
            offline, "--control-url", "http://gw", "--max-idle-runs", "6",
        )) == 0
    assert json.loads(offline["cursor"].read_text())["idle_runs"] == 3

    # The source answers again on the next run.
    offline["calls"]["fetch"].clear()

    def answering(client, targets, raw_store, **kwargs):
        targets = list(targets)
        return (
            [f"event-{tournament}-{season}" for tournament, season in targets],
            {"targets": len(targets), "pages": len(targets),
             "events": len(targets), "missing": 0, "truncated": 0,
             "foreign_season": 0, "resumed": 0, "chased": 0, "chase_settled": 0},
            [],
        )

    monkeypatch.setattr(refresh, "fetch_season_schedules", answering)
    monkeypatch.setattr(
        refresh, "fetch_season_fixtures",
        lambda client, targets, raw_store: (
            [f"fixture-{pair}" for pair in list(targets)],
            {"targets": len(list(targets)), "pages": len(list(targets)),
             "events": len(list(targets)), "missing": 0, "foreign_season": 0},
        ),
    )
    assert refresh.main(_argv(
        offline, "--control-url", "http://gw", "--max-idle-runs", "6",
    )) == 0
    report = json.loads(offline["output"].read_text())
    assert report["rows_written"] > 0 and report["idle_runs"] == 0
    assert json.loads(offline["cursor"].read_text())["idle_runs"] == 0


@pytest.mark.unit
def test_the_emptiness_guard_can_be_turned_off_and_never_blocks_a_start(
    offline, monkeypatch
):
    # ``--max-idle-runs 0`` disables it, and a garbled counter reads as zero:
    # this guard must never be the reason a healthy lane cannot run.
    offline["calls"]["known"] = {("SS-7", "2627")}
    _mute_everything(monkeypatch)
    offline["cursor"].write_text(json.dumps({
        "snapshot_id": SNAPSHOT["snapshot_id"], "targets": _digest(),
        "idle_runs": "many", "index": {},
    }))

    assert refresh.read_idle_runs(offline["cursor"]) == 0
    assert refresh.main(_argv(
        offline, "--control-url", "http://gw", "--max-idle-runs", "0",
    )) == 0
    assert json.loads(offline["output"].read_text())["idle_runs"] == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "members, limit",
    [
        (set(), 3),          # класс пуст
        ({4}, 3),            # один член, срез шире класса
        ({2, 4}, 2),         # класс ровно в размер среза, разрежен
        ({5, 6}, 2),         # класс ровно в размер среза, в хвосте
        ({1, 2, 3, 4}, 2),   # класс больше среза
        ({1, 2}, 0),         # срез нулевой (очередь недосева забрала весь seed)
    ],
)
def test_the_anchor_moves_whenever_the_class_outgrows_its_slice(members, limit):
    # Sol r12 #9 asked for an exact cursor rule and Sol r13 #2 showed the
    # obvious one is wrong: a class of exactly ``limit`` members scattered
    # through the sequence is walked WHOLE and still leaves the anchor
    # somewhere else, and a zero slice leaves it put with members waiting.
    # Only one direction holds, and it is the one acceptance needs: a class
    # that does not fit its slice is guaranteed to advance.
    sequence = [
        _target(index, 1, f"SS-{index}", "2627") for index in range(1, 7)
    ]
    wanted = lambda target: target.tournament_id in members  # noqa: E731
    start = sequence[0].pair

    picked, anchor = refresh.take_slice(sequence, start, limit, wanted)

    assert len(picked) == min(len(members), max(limit, 0))
    if len(members) > limit > 0:
        assert anchor != start
    # The converse is NOT asserted: an anchor that moved says nothing on its
    # own, which is why the report carries ``class_members`` next to it.


@pytest.mark.unit
def test_the_report_says_which_limit_each_class_was_sliced_with(
    offline, monkeypatch
):
    # Sol r13 #3: the seed slice is what is LEFT of --max-seed after the retry
    # queue took its share, so comparing the class against the configured
    # number compares it against a limit that was never used.  Sol r14 #2: and
    # asserting the arithmetic again proves nothing — the report has to be
    # checked against the limit ``take_slice`` was REALLY called with, or a
    # regression to ``args.max_seed`` stays green on a one-member fixture.
    limits: list[int] = []
    real_take_slice = refresh.take_slice

    def spy(targets, start, limit, wanted):
        limits.append(limit)
        return real_take_slice(targets, start, limit, wanted)

    monkeypatch.setattr(refresh, "take_slice", spy)
    offline["calls"]["known"] = {("SS-7", "2627")}
    offline["incomplete"].write_text(json.dumps({
        "seasons": [[23, 88001, 3, 1_700_000]], "cursor": None,
    }))

    assert refresh.main(_argv(
        offline, "--control-url", "http://gw", "--max-seed", "2",
    )) == 0

    report = json.loads(offline["output"].read_text())
    # The retry queue is sliced first, with the whole --max-seed; then the
    # three classes, in order, and seed gets what the queue left.
    queue_limit, *class_limits = limits
    assert queue_limit == 2
    assert dict(zip(refresh.SWEEP_CLASSES, class_limits)) == report["class_limits"]
    assert report["class_limits"]["seed"] == 1 == 2 - report["retry_targets"]
    assert report["class_limits"]["due"] == 150  # the configured default


@pytest.mark.unit
def test_the_estimate_counts_the_leases_that_shrink_with_the_budget(tmp_path):
    # Sol r14 #1: the client asks for min(per-lease ceiling, what is LEFT of
    # the budget), so the last leases of a run are smaller and serve fewer
    # pages each.  Dividing the payload by one fixed lease size undercounted
    # the warm-ups of exactly the plans that sit against the cap.
    # The knobs are EXACTLY the ones ``main`` is called with below: measuring
    # one plan and asking the runner for another is how the old version of this
    # test stayed green under a mutation (Sol r15 #3).
    max_due, max_stale, max_seed, chase_pages, seed_pages = 488, 1, 1, 1, 1
    knobs = (max_due, max_stale, max_seed, chase_pages, seed_pages)
    lease = 8 * 1024 * 1024
    # This plan is 496 pages; at a constant lease ceiling that is two warm-ups
    # and lands EXACTLY on the cap below, which is how it used to pass.
    flat = refresh.worst_case_bytes(*knobs, lease)
    assert refresh.worst_case_pages(*knobs) == 496

    honest = refresh.worst_case_bytes(*knobs, lease, flat)

    # With the shrinking ceiling the same plan needs more leases, so it does
    # NOT fit a cap equal to the flat estimate — and the preflight says so.
    assert honest > flat
    # NEVER /dev/null for these: the report, the cursor and the retry queue are
    # written with ``os.replace``, which would swap the device node for a plain
    # file and break every ``2>/dev/null`` on the machine (Sol r15 #1 — it did).
    output = tmp_path / "report.json"
    argv = [
        "--snapshot", str(tmp_path / "nonexistent.json"),
        "--output", str(output),
        "--cursor", str(tmp_path / "cursor.json"),
        "--incomplete", str(tmp_path / "incomplete.json"),
        "--control-url", "http://gw",
        "--max-due", str(max_due), "--max-stale", str(max_stale),
        "--max-seed", str(max_seed), "--chase-pages", str(chase_pages),
        "--seed-pages", str(seed_pages),
        "--per-lease-max-bytes", str(lease), "--budget-cap-bytes", str(flat),
    ]

    assert refresh.main(argv) == 1

    # And it is the CAP that stopped it, not the missing snapshot: the preflight
    # runs first and names the plan it refused (Sol r15 #3 — the old test asked
    # main for a different plan than the one it had measured, so dropping the
    # cap from the call still failed, for the wrong reason).
    report = json.loads(output.read_text())
    assert f"{refresh.worst_case_pages(*knobs)} pages" in report["errors"][0]
    assert "over the" in report["errors"][0]
    # And a plan that fits is unaffected: the default sweep costs the same as
    # it did before the leases were walked one by one.
    defaults = (
        refresh.DEFAULT_MAX_DUE, refresh.DEFAULT_MAX_STALE,
        refresh.DEFAULT_MAX_SEED, refresh.DEFAULT_CHASE_PAGES,
        refresh.DEFAULT_SEED_PAGES,
    )
    assert refresh.worst_case_bytes(*defaults, lease, refresh.DEFAULT_BUDGET_CAP_BYTES) == (
        refresh.worst_case_bytes(*defaults, lease)
    )


@pytest.mark.unit
def test_a_lease_carries_the_page_that_crosses_the_remint_mark_but_not_one_that_overflows():
    # Two limits bound a lease and the estimate has to honour BOTH.
    knobs = (1, 1, 1, 1, 1)
    assert refresh.worst_case_pages(*knobs) == 9

    # (a) The client checks the 90 % mark BEFORE the request, so the page that
    # crosses it rides the old lease.  Rounding that down said these nine pages
    # need two leases and refused a plan that fits (Sol r15 #2).
    roomy = 8 * 1024 * 1024
    one_lease = refresh._LEASE_WARMUP_BYTES + 9 * refresh._PAGE_BYTES
    assert refresh.worst_case_bytes(*knobs, roomy, 336_896) == one_lease == 330_752

    # (b) But the gateway stops at ``lease.max_bytes``, so a page that does not
    # FIT cannot be served whole, however far the mark is (Sol r16 #1) — and it
    # is not free either: the client, still under the mark, asks for it anyway
    # and the gateway serves the prefix that fits before cutting, leaving the
    # lease billed to its ceiling (Sol r17 #1; ``filter_proxy`` ``_pump``, whose
    # own test asserts ``lease.total_bytes == lease.max_bytes``).
    tight = 128 * 1024
    room_left = tight - refresh._LEASE_WARMUP_BYTES - refresh._PAGE_BYTES
    assert 0 < room_left < refresh._PAGE_BYTES  # one page fits, a second does not
    # Eight leases are drained to the ceiling by that cut-off attempt; the
    # ninth has nothing left to ask for and stops after its page.
    assert refresh.worst_case_bytes(*knobs, tight) == (
        8 * tight + refresh._LEASE_WARMUP_BYTES + refresh._PAGE_BYTES
    ) == 1_158_144
    # Which is why a cap sized as "nine clean leases" is refused: the run would
    # have burnt through it halfway.
    nine_clean_leases = 9 * (refresh._LEASE_WARMUP_BYTES + refresh._PAGE_BYTES)
    assert refresh.worst_case_bytes(*knobs, tight, nine_clean_leases) > nine_clean_leases

    # (c) The re-mint mark is 9/10 of the ceiling and the client compares with
    # ``>=``, so a FRACTIONAL mark decides the same way an integer one does not:
    # at 121 743 bytes the mark is 109 568.7 and the client sits on 109 568 —
    # below it — so it asks for another page and burns the lease out.  Rounding
    # the mark down to 109 568 called that a clean re-mint and accepted a cap
    # the run cannot live within (Sol r18 #1).
    fractional = 121_743
    assert refresh._LEASE_WARMUP_BYTES + refresh._PAGE_BYTES < 0.9 * fractional
    assert refresh.worst_case_bytes(*knobs, fractional) == (
        8 * fractional + refresh._LEASE_WARMUP_BYTES + refresh._PAGE_BYTES
    ) == 1_083_512
    assert refresh.worst_case_bytes(*knobs, fractional, nine_clean_leases) > (
        nine_clean_leases
    )


@pytest.mark.unit
def test_a_lease_that_would_end_exactly_on_its_ceiling_is_refused(offline, tmp_path):
    # Sol r19/r20: a lease drained to its final byte never lets the gateway read
    # the provider EOF — the down pump takes the allowance check at the top of
    # its loop, finds nothing left and breaks before the zero-length read, so
    # the lease is closed accounting-uncertain with its escrow retained.  It can
    # happen on ANY lease of the plan, so the layout is refused outright.
    lease = 320_000
    with pytest.raises(ValueError, match="exactly on its ceiling"):
        refresh.worst_case_bytes(1, 1, 1, 1, 1, lease, 412_672)
    # One byte of slack changes the layout and the same plan is fine.
    assert refresh.worst_case_bytes(1, 1, 1, 1, 1, lease, 412_673) == 412_672
    # And it is not only the last, budget-shrunk lease that matters: here the
    # FIRST lease of the plan is the one that would land on its ceiling, and a
    # check that only looked at the tail would let this through (Sol r21 #6).
    with pytest.raises(ValueError, match="exactly on its ceiling"):
        refresh.worst_case_bytes(1, 1, 1, 1, 1, 275_456, 412_673)

    output = tmp_path / "report.json"
    argv = [
        "--snapshot", str(tmp_path / "nonexistent.json"),
        "--output", str(output),
        "--cursor", str(tmp_path / "cursor.json"),
        "--incomplete", str(tmp_path / "incomplete.json"),
        "--control-url", "http://gw",
        "--max-due", "1", "--max-stale", "1", "--max-seed", "1",
        "--chase-pages", "1", "--seed-pages", "1",
        "--per-lease-max-bytes", str(lease), "--budget-cap-bytes", "412672",
    ]

    assert refresh.main(argv) == 1

    # And the run says so before a single paid request, not halfway through.
    report = json.loads(output.read_text())
    assert "exactly on its ceiling" in report["errors"][0]
    # Refused before a single paid request: no client was ever built.
    assert _FakeClient.created == []
