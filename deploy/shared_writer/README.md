# Shared writer release for ClubElo #1465

Preparation only. No merge, delivery, pause change or ingestion is authorized by
this PR. This entrypoint releases **only** `scrapers/base/iceberg_writer.py` with
SHA256 `a1d2d2dfa33b496ca891e0752560099ac0b99fef71146c67e4a1d7f09970e9c4`
from before SHA256
`5ab88ead5d77317f17dde11db8033d79501c6ebd0a48fd5b6eb60efc70ad24d3`.
It is deliberately a bounded release, not a general deployment framework.

`release.py` never edits runtime locks, trust roots, configs or another module;
never runs Compose, live Python, DAG tasks, pause/unpause, data writes or source
requests. It runs from an isolated checkout, outside the mounted production tree.
`host.py` reads selected Docker fields, config hashes and Airflow metabase.
No environment secrets are read or written into evidence.

## Order requiring separate owner authorization

1. Review and merge the automation and writer #1627 only after required CI.
   #1623 already supplied the Buildx repair; no duplicate CI change is needed.
   Before writer merge, coordinate WhoScored's independent night delivery into
   `/root/whoscored-1017-runtime/src`; shared source gates in ClubElo/Understat
   will block while their common writer lags. Do not extend `ALLOWED_LAG`.
2. Agree a maintenance window for **all active DAGs in the shared metabase** and
   external collection/manual delivery drivers. Pausing those DAGs is a separate
   operational action and needs authorization; this runner does not do it.
   Record their original pause map for later separately approved restoration.
   Drain outstanding runs/tasks. Do not kill someone else's process.
   Known driver detection is a guard, not a lock against future manual launches:
   operators must observe the agreed window. ClubElo's 04:30–05:25 МСК window
   alone does not authorize common work; it overlaps WhoScored 05:00–08:00 МСК.
3. Capture a new bundle **after** maintenance preparation, using the final full
   merged commit SHA, not the original PR SHA if squash-merged. Use a private
   external directory on the same filesystem as `/root/dpf-whoscored-merge`.
   Explicit UTC epoch bounds must describe the agreed window (max one hour).
   Example commands, to run from the reviewed isolated checkout:

   ```bash
   python3 -B -m deploy.shared_writer.release prepare \
     --bundle /root/shared-writer-releases/RELEASE_ID \
     --repo /root/data-platform-football --commit FULL_MERGE_SHA \
     --start START_UTC_EPOCH --end END_UTC_EPOCH
   python3 -B -m deploy.shared_writer.release rehearse \
     --bundle /root/shared-writer-releases/RELEASE_ID --approval MANIFEST_SHA256
   python3 -B -m deploy.shared_writer.release check \
     --bundle /root/shared-writer-releases/RELEASE_ID --approval MANIFEST_SHA256
   ```

   Create only the external parent directory beforehand. `prepare` prints the
   manifest digest; inspect the manifest and rehearsal logs. The bundle pins
   current source content/modes/owners, four consumers' IDs/images/mounts, all
   actual Compose config hashes, metabase identity and DAG pause map. Original
   live contract/lock stay in the copy, not the repository's regenerated lock.
   Rehearsal runs 4 before + 4 after + 4 rollback fresh processes using exact
   images, network disabled, read-only copied code, limited resources. It checks
   startup anchor, writer/ClubElo/WhoScored/filter imports and real PyIceberg
   Summary. It is not a full DagBag, scheduler or Lakekeeper write test.
   Any code, container, config or tool drift requires a fresh reviewed bundle.
4. Obtain owner approval tied to that manifest/window; passing `--approval` is
   an identity check, not permission. Then run the normal entrypoint once:

   ```bash
   python3 -B -m deploy.shared_writer.release apply \
     --bundle /root/shared-writer-releases/RELEASE_ID --approval MANIFEST_SHA256
   ```

   The runner fetches master and requires the payload commit to be merged and
   master still to contain this exact writer. It holds shared + six existing
   source delivery flocks, refuses inflight markers, checks quiescence again,
   fsyncs a saved original/staged file and journal, then performs one atomic
   rename. Source locks are not removed or truncated. No restart is required by
   the rehearsed current images. Check actual configuration again if images move.
5. Accept the shared release only when journal is `accepted`: original
   neighbors/locks/images/mounts and DAG pauses preserved, healthy containers,
   no unfinished tasks/import errors/drivers, every active DAG parsed after
   publication + 60 seconds. Deadline 420 seconds. This is scheduler/metabase
   evidence; standalone imports alone cannot mark acceptance.
6. Keep ClubElo paused. Shared release acceptance does not finish #1465.
   Separately authorize restoration of other paused sources and ClubElo unpause
   with addressed `EXPECTED_RUNNING` backup. Verify a successful **daily write**
   and `validate_data`, then history in an agreed quiet window, then **three
   successful new-code runs**. #1465 remains open until that acceptance.

## Failure and recovery

Postflight failure attempts an automatic rollback, validates the original
writer and waits for scheduler reparse again. The command still exits nonzero
because delivery failed; `rolled_back` distinguishes healthy restoration from
`blocked`. Code rollback does not revert Iceberg data. Never use ClubElo's own
rollback to restore this shared file.

The durable journal precedes rename. A common `shared-writer-inflight.json`
marker also survives process death and blocks another bundle until recovery.
After a crash/host interruption or for an
explicitly authorized rollback of an accepted release:

```bash
python3 -B -m deploy.shared_writer.release recover \
  --bundle /root/shared-writer-releases/RELEASE_ID --approval MANIFEST_SHA256
```

`rollback` is an alias. Recovery is allowed outside the original time window
for containment, but still requires held locks, unchanged identity, maintenance
quiescence and a matching before/after writer. New external work must first be
coordinated. The runner refuses to overwrite third-party changes or claim a
successful rollback when health/reparse verification fails. Inspect `state.json`
and evidence; do not delete a blocked journal, break locks or force replacement.
No automatic retry, cron installation, notification or unpause is included.

## Validation boundaries

Unit tests cover real filesystem publication/old-reader visibility, preservation
of adjacent files/owner/mode, failed postflight, interrupted publication/recovery,
foreign drift, lock contention, corrupt bundles, unsafe paths and host gates.
The disposable-container rehearsal proves startup/import compatibility in copies.
Actual shared delivery, real daily write, history and three-run acceptance remain
unperformed until their separate authorization.
