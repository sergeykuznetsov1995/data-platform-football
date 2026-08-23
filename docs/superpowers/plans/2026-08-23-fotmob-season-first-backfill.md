# FotMob Season-First Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the automatic FotMob history backfill finish the newest season cycle before it can start an older one.

**Architecture:** Add one deterministic season-cycle key in the FotMob planner. Automatic `BACKFILL` work in the `HISTORY` lane will select only the newest unfinished cycle; explicit/manual scope requests, daily work, current-season work, and replay keep their existing behavior. A cycle groups calendar labels such as `2025` with split labels ending in the same year such as `2024/2025`.

**Tech Stack:** Python 3.11, pytest, existing `scrapers.fotmob.planner` domain types.

## Global Constraints

- Preserve exact FotMob `source_season_key` values; the new key controls scheduling only.
- Apply the barrier only to automatic `RunMode.BACKFILL` + `ScopeLane.HISTORY` plans.
- A retry or terminal failure in the newest cycle blocks older cycles until it becomes runnable and settles.
- A `source_gap` inside its existing review TTL remains settled for scheduling purposes.
- Do not change daily/current/replay ordering or explicit-scope behavior.

---

### Task 1: Define a global FotMob history-season cycle

**Files:**
- Modify: `scrapers/fotmob/planner.py`
- Modify: `tests/unit/scrapers/test_fotmob_planner.py`

**Interfaces:**
- Consumes: exact FotMob `source_season_key` strings.
- Produces: `_history_season_cycle_key(source_season_key: str) -> tuple[int, str]`, which groups calendar and split-year labels by their final year.

- [ ] **Step 1: Write the failing cycle-key test**

```python
def test_history_season_cycle_groups_calendar_and_split_year_labels():
    assert _history_season_cycle_key("2024/2025") == (2025, "")
    assert _history_season_cycle_key("2025") == (2025, "")
    assert _history_season_cycle_key("2023/2024") < (2025, "")
    assert _history_season_cycle_key(" Apertura ") == (-1, "apertura")
```

- [ ] **Step 2: Run the focused test and confirm it fails**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers/test_fotmob_planner.py::test_history_season_cycle_groups_calendar_and_split_year_labels -q`

Expected: collection fails because `_history_season_cycle_key` does not exist.

- [ ] **Step 3: Implement the deterministic cycle key**

```python
import re


def _history_season_cycle_key(source_season_key: str) -> tuple[int, str]:
    label = str(source_season_key).strip()
    years = [int(value) for value in re.findall(r"(?<!\d)[12]\d{3}(?!\d)", label)]
    if years:
        return max(years), ""
    return -1, label.casefold()
```

This groups `2024/2025` and `2025` while preserving the exact stored labels. Opaque labels are deterministic and run after numeric cycles.

- [ ] **Step 4: Run the planner tests**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers/test_fotmob_planner.py -q`

Expected: all planner tests pass.

- [ ] **Step 5: Format and commit the cycle-key unit**

Run: `/root/.venvs/dpf-test/bin/ruff format --check scrapers/fotmob/planner.py tests/unit/scrapers/test_fotmob_planner.py`

Run: `git diff --check`

Commit:

```bash
git add scrapers/fotmob/planner.py tests/unit/scrapers/test_fotmob_planner.py
git commit -m "refactor(fotmob): derive global history season cycles"
```

### Task 2: Enforce the automatic history season barrier

**Files:**
- Modify: `scrapers/fotmob/planner.py`
- Modify: `tests/unit/scrapers/test_fotmob_planner.py`

**Interfaces:**
- Consumes: `_history_season_cycle_key(source_season_key: str) -> tuple[int, str]`, `plan_seasons(...) -> list[SeasonWorkItem]`, and existing durable attempt states.
- Produces: automatic history plans containing only the newest unfinished season cycle.

- [ ] **Step 1: Write failing newest-cycle tests**

```python
def test_automatic_history_plans_only_the_newest_unfinished_season_cycle():
    plan = plan_seasons(
        [_classified(47), _classified(48), _classified(49)],
        [
            SeasonRef(47, "2024/2025", source_order=1),
            SeasonRef(48, "2025", source_order=1),
            SeasonRef(49, "2023/2024", source_order=2),
        ],
        mode=RunMode.BACKFILL,
        lane=ScopeLane.HISTORY,
    )
    assert {item.identity for item in plan} == {
        (47, "2024/2025"),
        (48, "2025"),
    }


def test_automatic_history_advances_after_the_newest_cycle_is_complete():
    plan = plan_seasons(
        [_classified(47), _classified(48), _classified(49)],
        [
            SeasonRef(47, "2024/2025", source_order=1),
            SeasonRef(48, "2025", source_order=1),
            SeasonRef(49, "2023/2024", source_order=2),
        ],
        mode=RunMode.BACKFILL,
        lane=ScopeLane.HISTORY,
        previously_successful={(47, "2024/2025"), (48, "2025")},
    )
    assert [item.identity for item in plan] == [(49, "2023/2024")]
```

- [ ] **Step 2: Add strict-barrier and manual-bypass tests**

```python
def test_automatic_history_retry_in_newest_cycle_blocks_older_cycles():
    now = datetime(2026, 8, 23, 10)
    attempts = {
        (47, "2024/2025"): ScopeAttemptState(
            competition_id=47,
            source_season_key="2024/2025",
            plan_signature="fmplan1-test",
            attempt_count=1,
            last_attempt_at=now,
            next_retry_at=now + timedelta(hours=1),
            outcome="retryable",
            reason="HTTP 503",
        )
    }
    plan = plan_seasons(
        [_classified(47), _classified(49)],
        [
            SeasonRef(47, "2024/2025", source_order=1),
            SeasonRef(49, "2023/2024", source_order=2),
        ],
        mode=RunMode.BACKFILL,
        lane=ScopeLane.HISTORY,
        attempt_states=attempts,
        now=now,
    )
    assert plan == []


def test_explicit_history_scopes_are_not_reduced_to_one_cycle():
    plan = plan_seasons(
        [_classified(47)],
        [
            SeasonRef(47, "2024/2025", source_order=1),
            SeasonRef(47, "2023/2024", source_order=2),
        ],
        mode=RunMode.BACKFILL,
        lane=ScopeLane.HISTORY,
        explicit_scopes={(47, "2024/2025"), (47, "2023/2024")},
    )
    assert [item.identity for item in plan] == [
        (47, "2024/2025"),
        (47, "2023/2024"),
    ]
```

- [ ] **Step 3: Run the focused tests and confirm they fail for the missing policy**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers/test_fotmob_planner.py -q`

Expected: the new automatic-history assertions fail because older seasons are still present; existing tests pass.

- [ ] **Step 4: Split candidate selection from runnable-attempt filtering**

Collect eligible, deduplicated `(SeasonRef, ScopeAttemptState | None)` pairs after lane, mode, completed-scope, successful-attempt, and fresh-`source_gap` filtering. Before terminal/retry cooldown filtering, select the maximum `_history_season_cycle_key(...)` only when all of these are true:

```python
mode == RunMode.BACKFILL
and lane == ScopeLane.HISTORY
and explicit_scopes is None
```

Then apply the existing terminal TTL and `next_retry_at` checks to that selected cycle. This deliberately returns an empty plan when the newest unfinished cycle is cooling down, so older cycles cannot leapfrog it.

- [ ] **Step 5: Preserve the existing in-cycle fairness order**

Keep each `SeasonWorkItem.priority` as:

```python
(
    active_rank,
    last_attempt,
    recency,
    season.source_season_key,
)
```

The new barrier chooses the cycle; the existing priority continues to rotate fairly among competitions inside that cycle.

- [ ] **Step 6: Run focused tests**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers/test_fotmob_planner.py -q`

Expected: all planner tests pass.

- [ ] **Step 7: Run FotMob unit tests**

Run: `/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers/test_fotmob_*.py tests/unit/scripts/test_fotmob_backfill.py -q`

Expected: all selected FotMob tests pass with zero failures.

- [ ] **Step 8: Check formatting and commit**

Run: `/root/.venvs/dpf-test/bin/ruff format --check scrapers/fotmob/planner.py tests/unit/scrapers/test_fotmob_planner.py`

Run: `git diff --check`

Commit:

```bash
git add docs/superpowers/plans/2026-08-23-fotmob-season-first-backfill.md \
  scrapers/fotmob/planner.py \
  tests/unit/scrapers/test_fotmob_planner.py
git commit -m "feat(fotmob): finish newer history seasons first"
```
