"""Bounded lock waiting around the sealed FBref janitor."""

import pytest


@pytest.fixture
def fbref_lock_wait(monkeypatch):
    """Exercise the real maintenance wrapper with an active ingest owner."""
    from types import SimpleNamespace

    import utils.maintenance_tasks as maintenance
    import utils.fbref_maintenance as waiting
    from scrapers.fbref.control import ControlStore, StateConflict

    state = SimpleNamespace(
        now=0.0, sleeps=[], events=[], owner="ingest", release_at=None,
        acquire_error=None, fence_error=None, oversleep=0.0, raced=False,
        race_once=False, probes=0,
    )

    def sleep(seconds):
        assert state.owner == "ingest"
        assert "janitor" not in state.events
        state.sleeps.append(seconds)
        state.now += seconds + state.oversleep
        if state.release_at is not None and state.now >= state.release_at:
            state.owner = None

    class FakeControl:
        def get_publication_lock(self):
            state.probes += 1
            return {"active": state.owner is not None}

        def create_run(self, *_args, **kwargs):
            assert kwargs["request_limit"] == kwargs["byte_limit"] == 0
            state.events.append("create")
            return "maintenance"

        def start_run(self, run_id):
            assert run_id == "maintenance"
            state.events.append("start")

        def acquire_publication_lock(self, run_id, **kwargs):
            state.events.append("acquire")
            assert kwargs["ttl_seconds"] == 3600
            if state.race_once and not state.raced:
                state.raced = True
                state.owner = "ingest"
            if state.acquire_error:
                raise state.acquire_error
            if state.owner:
                raise StateConflict(
                    "FBref publication is locked by another control run"
                )
            state.owner = run_id

        def renew_publication_lock(self, run_id, **_kwargs):
            assert state.owner == run_id
            state.events.append("renew")

        def assert_publication_lock_owner(self, run_id, **_kwargs):
            state.events.append("assert")
            if state.fence_error:
                raise state.fence_error
            assert state.owner == run_id

        def release_publication_lock(self, run_id):
            assert state.owner == run_id
            state.events.append("release")
            state.owner = None

        def finish_run(self, _run_id, *, succeeded):
            state.events.append(("finish", succeeded))

        def get_observation_cleanup_evidence(self, _refresh):
            return None

        def get_run(self, _run):
            return None

    def janitor(**kwargs):
        assert state.owner == "maintenance"
        state.events.append("janitor")
        kwargs["before_drop"]("stage", "refresh")
        state.events.append("drop")
        return {"attention_required_count": 0, "mode": "apply"}

    monkeypatch.setattr(ControlStore, "from_env", FakeControl)
    monkeypatch.setattr(maintenance, "janitor_fbref_generic_stages", janitor)
    monkeypatch.setattr(
        waiting, "time", SimpleNamespace(monotonic=lambda: state.now, sleep=sleep),
        raising=False,
    )
    monkeypatch.setattr(waiting, "FBREF_JANITOR_LOCK_WAIT_SECONDS", 65)
    return waiting, state


@pytest.mark.unit
def test_fbref_janitor_waits_for_ingest_release_then_fences_drop(fbref_lock_wait):
    maintenance, state = fbref_lock_wait
    state.release_at = 60

    result = maintenance.maintain_fbref_stages_with_lock_wait(mode="apply")

    assert state.sleeps == [30, 30]
    assert state.events == [
        "create", "start", "acquire", "janitor",
        "renew", "assert", "drop", "release", ("finish", True),
    ]
    assert result["control_run_id"] == "maintenance"


@pytest.mark.unit
@pytest.mark.parametrize("oversleep, expected_sleeps", [(0, [30, 30, 5]), (100, [30])])
def test_fbref_janitor_timeout_leaves_ingest_lock_intact(
    fbref_lock_wait, oversleep, expected_sleeps,
):
    maintenance, state = fbref_lock_wait
    state.oversleep = oversleep

    with pytest.raises(TimeoutError, match="FBref.*publication lock"):
        maintenance.maintain_fbref_stages_with_lock_wait(mode="apply")

    assert state.owner == "ingest"
    assert state.sleeps == expected_sleeps
    assert state.events.count("acquire") == 0
    assert "janitor" not in state.events
    assert "release" not in state.events
    assert state.events == []


@pytest.mark.unit
@pytest.mark.parametrize("error_type", ["state", "database"])
def test_fbref_janitor_does_not_wait_on_other_errors(fbref_lock_wait, error_type):
    from scrapers.fbref.control import StateConflict

    maintenance, state = fbref_lock_wait
    error = StateConflict("Publication lock owner must be an existing running run")
    if error_type == "database":
        error = RuntimeError("database unavailable")
    state.acquire_error = error
    state.owner = None

    with pytest.raises(type(error), match=str(error)):
        maintenance.maintain_fbref_stages_with_lock_wait(mode="apply")

    assert state.sleeps == []
    assert state.owner is None
    assert state.events == ["create", "start", "acquire", ("finish", False)]


@pytest.mark.unit
def test_fbref_janitor_preserves_owner_fence_after_wait(fbref_lock_wait):
    from scrapers.fbref.control import StateConflict

    maintenance, state = fbref_lock_wait
    state.release_at = 30
    state.fence_error = StateConflict("publication owner fence failed")

    with pytest.raises(StateConflict, match="owner fence failed"):
        maintenance.maintain_fbref_stages_with_lock_wait(mode="apply")

    assert state.sleeps == [30]
    assert "drop" not in state.events
    assert state.events[-2:] == ["release", ("finish", False)]


@pytest.mark.unit
def test_fbref_janitor_free_lock_runs_immediately(fbref_lock_wait):
    maintenance, state = fbref_lock_wait
    state.owner = None

    maintenance.maintain_fbref_stages_with_lock_wait(mode="apply")

    assert state.sleeps == []
    assert state.probes == 1
    assert state.events[-4:] == ["assert", "drop", "release", ("finish", True)]


@pytest.mark.unit
def test_fbref_janitor_retries_atomic_acquire_race(fbref_lock_wait):
    maintenance, state = fbref_lock_wait
    state.owner = None
    state.race_once = True
    state.release_at = 30

    maintenance.maintain_fbref_stages_with_lock_wait(mode="apply")

    assert state.sleeps == [30]
    assert state.events == [
        "create", "start", "acquire", ("finish", False),
        "create", "start", "acquire", "janitor", "renew", "assert", "drop",
        "release", ("finish", True),
    ]


@pytest.mark.unit
def test_fbref_janitor_race_timeout_does_not_release_new_owner(fbref_lock_wait):
    maintenance, state = fbref_lock_wait
    state.owner = None
    state.race_once = True

    with pytest.raises(TimeoutError, match="FBref.*publication lock"):
        maintenance.maintain_fbref_stages_with_lock_wait(mode="apply")

    assert state.owner == "ingest"
    assert state.sleeps == [30, 30, 5]
    assert state.events == ["create", "start", "acquire", ("finish", False)]
