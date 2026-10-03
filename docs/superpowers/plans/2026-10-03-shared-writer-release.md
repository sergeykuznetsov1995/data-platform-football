# Shared writer release for ClubElo #1465

Goal: prepare a reviewed, tested release entrypoint for the single writer property
rename. No merge, live delivery, pause changes or ingestion in this task.

Architecture: immutable bundle binds the exact Git payload, live code closure,
container/config identities and time window. A journaled executor holds source
delivery locks, replaces one regular file atomically, and verifies scheduler
reparse. Failure restores the saved file only if the target still belongs to this
release. Recovery handles process death before/after rename. Live contract and
lock are immutable. An isolated copy rehearses both images/startup anchors.

Tech stack: Python standard library, Docker read-only inspection/psql, pytest;
existing source delivery scripts remain unchanged.

1. Check existing CI repair; reuse merged #1623 and rebase #1627, verify exact
   patch and required CI. Do not create a duplicate CI PR.
2. Implement `deploy/shared_writer/release.py`, read-only `host.py`, startup probe
   and runbook. Prepare is read-only against runtime. Apply requires a fresh
   merged commit, approved manifest digest and rehearsed exact bundle.
3. Enforce a separately coordinated maintenance window: all active shared DAGs
   already paused, zero outstanding runs/tasks, no known external driver, locks
   held. The executor never pauses/resumes sources or stops processes.
4. Test atomic publication, unchanged neighbors/modes/owners, failure rollback,
   interrupted-journal recovery, drift refusal, unsafe paths, expired window,
   source lock contention, and fresh scheduler parse gates. Run full unit suite.
5. Separate read-only review of exact diff/evidence; correct material findings.
   Publish preparation PR, update #1627 evidence and handoff. Owner permission
   remains required for merge, delivery, unpause and history.

Acceptance remains: successful daily write, history in an agreed quiet window,
three successful new-code runs; #1465 stays open.
