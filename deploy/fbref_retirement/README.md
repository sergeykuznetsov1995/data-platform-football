# Bounded FBref legacy Silver containment (#1465 / #1634)

Preparation only. No production pause, database update, file delivery, service
restart, source request or archive change is performed by these tools.

## Source candidate

The full retirement is already in master. This package is a narrow patch to the
exact deployed pre-retirement producer versions. It removes only the legacy
Silver trigger in current/backfill/replay, preserves Bronze export and its
fail-closed lock finalizer, and leaves the old Silver DAG/SQL and shared modules
in place. The factory also serves bootstrap; that topology remains unchanged.
The publication-order marker becomes `fbref-current-silver-retired-v1`.

| File | Before SHA256 | After SHA256 |
|---|---|---|
| `dags/utils/fbref_current_dag_factory.py` | `e6daffedc7b9546bb39e21dceb69f561d40cfe1afb1dfd8983d632ab0a8945a5` | `f25a38606508a6361ef959e3b66fb806148c1c785a8cd636954bc459e392ab0c` |
| `dags/dag_backfill_fbref.py` | `08cf3e959a9343b0c3384dff67570e30dd0a7e777591d44eb31a1d454a8ec27c` | `03145037ce71ff275aff68597b746c68d7159b3bdc575046416bbf9d2454a398` |
| `dags/dag_replay_fbref.py` | `d960cd6d8b331ecd073b4dcaea878424108cdabf813bdb64fd196f74f1e20e54` | `514ebef6a056fcf60cda85cc66d52d9438773eb4282ca59a49e35272849e2fc1` |

The unchanged `dags/utils/fbref_pipeline_tasks.py` is pinned to SHA256
`c0b13356ef737602c748f651637ed4cb12f84c50556893b6d88335daeceda1c9`.
Other undelivered FBref work (#1323/#1328/#1696), source schema migrations,
common `silver_tasks.py`, locks, images and budgets are outside this package.
Do not copy these files from master or apply the whole retirement commit.

Run from an isolated worktree. This command reads source bytes; it never imports
or writes production code. It copies originals and patched outputs to a new
private directory, verifies exact before hashes and refuses drift.

```bash
python3 -B -m deploy.fbref_retirement.prepare payload \
  --source /root/dpf-whoscored-merge \
  --output /root/data-platform-football/.local/tasks/1465/NEW_SOURCE_CANDIDATE
```

Its manifest remains `execution_qualified=false` and
`production_execution_authorized=false`. It is an input to a separately
reviewed coordinated delivery, not an executor. Startup compatibility must be
checked on all exact consumer images with the original locks, public catalog
inputs, all active DAGs, each forward/reverse prefix and unchanged source
behavior. `probe.py` runs only on a copied `/opt/airflow` tree with disposable
Airflow state, without network. It exercises the actual untouched live
finalizer for export success/failure, acquisition failure, canary, dry run,
bootstrap, nonpublishing backfill and replay. The graph comparison also checks
that defaults, budgets, existing tasks and unrelated edges stay unchanged.

## Queue candidate

`snapshot.sql` is a repeatable-read READ ONLY transaction ending in ROLLBACK.
It captures complete queued Silver DagRun/task rows and their exact parents.
Keep raw evidence private; do not unpickle `conf` or send raw records to Miro.
A snapshot on an active host is preparation evidence, not admission to execute.

```bash
# Read only; in the separately authorized source scope.
docker exec -i postgres psql -X -qAt -U airflow -d airflow \
  -v ON_ERROR_STOP=1 < deploy/fbref_retirement/snapshot.sql > SNAPSHOT_JSON
python3 -B -m deploy.fbref_retirement.prepare queue \
  --snapshot SNAPSHOT_JSON \
  --output /root/data-platform-football/.local/tasks/1465/NEW_QUEUE_CANDIDATE
```

The resulting `rehearsal.sql` always rolls back. It locks the four Airflow
metadata tables with NOWAIT and checks:

- every active shared DAG paused, and the three producers, Silver and five
  retirement hold DAGs present, active and paused;
- no nonterminal/unknown DagRun other than the exact queued Silver list;
- complete exact before-images for every queued child, its tasks and parents;
- every child unstarted/unended with clear_number=0 and no creating job; every
  task has try_number=0, NULL state/job/start/end and no queue/executor/trigger
  handoff, continuation, pid or duration evidence; missing fields also refuse;
- no executing, waiting, orphan/unknown task or unsafe job reference;
- no unfinished, inconsistent, unknown or recent LocalTaskJob.

The job/task guards are conservative: they can reject old abandoned metadata
that another classifier regards as historical. Never waive that refusal or
change unrelated metadata to satisfy it; investigate and review separately.
Under the same locks, only the approved child DagRuns receive state=failed and
end_date. Airflow has no cancelled DagRun state; failed honestly records the
explicit retirement cancellation, not a successful Silver publication. The
instrument never changes task/job/parent/history/control-store rows. Before
commit any error rolls back the transaction completely. After commit do not
requeue retired children automatically. For an unknown outcome, inspect exact
after rows and unchanged task/history fingerprints before any next action.

## Rendering commit SQL

There is no production connection or automatic apply command. After a fresh
snapshot and a separately approved exact manifest/window, an approval file
must contain exactly these fields:

```json
{
  "action": "close-exact-never-started-fbref-silver-runs",
  "manifest_sha256": "EXACT_QUEUE_MANIFEST_SHA256",
  "renderer_sha256": "EXACT_RENDER_COMMIT_PY_SHA256",
  "window_start": "OWNER_APPROVED_UTC_WITH_TIMEZONE",
  "window_end": "OWNER_APPROVED_UTC_WITH_TIMEZONE",
  "owner_evidence": "REFERENCE_TO_THE_EXPLICIT_OWNER_APPROVAL"
}
```

```bash
python3 -B -m deploy.fbref_retirement.render_commit \
  --bundle EXACT_QUEUE_CANDIDATE --approval OWNER_APPROVAL_JSON \
  --output /root/data-platform-football/.local/tasks/1465/NEW_APPROVED_SQL
```

The renderer verifies exact bundle/tool/snapshot hashes, IDs/counts and a window
of at most one hour. The snapshot must be at most five minutes old. PostgreSQL
repeats the time/freshness checks under locks and again immediately before
COMMIT. Rendering is not execution or approval. Never edit rehearsal SQL to
replace ROLLBACK or use a synthetic test authorization on production.

Production execution additionally requires its reviewed operator procedure:
shared/source delivery locks and inflight fencing, exact runtime/image/mount
identities, no external drivers or concurrent operators, backup/evidence,
postcommit verification and a defined failure/restore boundary. This package
does not implement or waive these external admission checks. The existing
common runner handles exactly two common files and cannot publish this
three-file source candidate.

## Coordinated window and acceptance

Wait for the current paid Bronze to terminate naturally before pausing its DAG;
pausing it earlier can stop remaining task scheduling. A new live run postpones
the window. Never kill workers or forcibly release their publication lock.
After separately approved pauses, freeze a fresh queue manifest (the current
run can still add a child), rehearse it on a disposable PostgreSQL and close
only the never-started list. Wait for normal jobs/drivers to drain. Keep Silver
and the five publication holds paused. Publish the qualified source candidate
and the common #1675 package through their separately approved guarded paths.
Restore permitted producer pauses only after confirming trigger containment.
If a source delivery fails, use the agreed rollback/hold boundary; blindly
restoring the old producer will refill the queue. Keeping pauses beyond the
window also needs an explicit contingency authorization.

Accept source delivery only with exact hashes on every consumer and fresh
scheduler topology, then observe one naturally successful publishing Bronze
with export/lock release and no new Silver child. Common delivery still needs
its own zero-work/driver checks and post-apply evidence. ClubElo delivery keeps
its regular window and gates; #1466/#1702 runtime acceptance and mass history
remain separate. The expired 10 October window cannot authorize these actions.

## Tests

- Unit preparation and approval tests run in the isolated checkout.
- Real SQL tests use only a disposable labelled PostgreSQL container, no DSN,
  network, production mounts or published ports:
  `FBREF_RETIREMENT_TEST_CONTAINER=FULL_OWNED_CID pytest tests/integration/test_fbref_retirement_sql.py -q`.
- Project full unit suite and independent read-only review are required before PR.

See the owner-approved [retirement decision](../../docs/operations/fbref-silver-retirement-1634.md).
