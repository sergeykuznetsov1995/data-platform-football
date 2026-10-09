# ClubElo #1465: bounded shared dependency release

Preparation is authorized. Merge, production maintenance, publication and history
remain separate owner decisions. This runner publishes exactly two already merged
files; it does not change ClubElo's auto-deliver script or `ALLOWED_LAG`:

| File | Before SHA256 | After SHA256 |
|---|---|---|
| `dags/utils/medallion_config.py` | `07b782cfd39d1edca87f66df79040f9e8924377f6eacf91bef3d02dff9493e91` | `b3cde46db5fa568152b1a8e3c8a7775dc8b327b51ae7a323efeb21878692eb1e` |
| `scrapers/utils/proxy_manager.py` | `3ee21c9045090046bd2f9509a038fbc2bc815e3cd1b67006841404bdd57b564b` | `d10e6da14d1ee79c9a8233e1b20669ea735463c652a59bb7525faf88df5d11e0` |

The medallion change rejects unproven `team_count_pending` evidence in the
SofaScore workload planner. Proxy selection gains `Proxy.http_url` and the
keyword-only `excluded_http_urls` arguments used by WhoScored. ClubElo does not
call either changed behavior, but its shared gate correctly blocks both live
versions. Aligning only medallion would expose the second blocker.

## Preparation and authorization

Run only from an isolated checkout. Keep evidence and bundles below
`/root/data-platform-football/.local/tasks/1465/`; never import production Python.
Read the project AGENTS and shared-stack protocol first.

1. Recheck the merged payload commit, actual shared images/mounts/config files,
   source drivers and Airflow metabase. Coordinate a maintenance window of at
   most one hour. **Pausing all active shared DAGs and later restoring their
   original map require a separately authorized operational window.** This
   runner performs neither action and never kills someone else's process.
2. Prepare from that final state. The manifest pins all runtime Python, original
   locks and `.airflowignore`, public medallion/SofaScore/FotMob catalog inputs,
   their owner/mode, consumer identities, Compose hashes, pause map, legacy-error
   policy and the complete release-tool closure. It refuses unexpected payloads.
3. Rehearse and check. Preparation on an unpaused or busy host is a **candidate**,
   not a ready release. Its rehearsal is useful but its `check`/`apply` must fail.
   Preparing the final maintenance map changes the digest and requires a fresh
   bundle, rehearsal and owner approval. Never edit a manifest in place.

```bash
python3 -B -m deploy.shared_dependencies.release prepare \
  --bundle /root/data-platform-football/.local/tasks/1465/releases/DEPENDENCY_RELEASE \
  --repo /root/data-platform-football --commit FULL_MERGED_PAYLOAD_SHA \
  --start AGREED_START_UTC_EPOCH --end AGREED_END_UTC_EPOCH
python3 -B -m deploy.shared_dependencies.release rehearse \
  --bundle BUNDLE_PATH --approval MANIFEST_SHA256
python3 -B -m deploy.shared_dependencies.release check \
  --bundle BUNDLE_PATH --approval MANIFEST_SHA256
```

The default requires zero import errors. The optional prepare-only
`--import-error-policy whoscored-legacy-20261003` reuses the **exact** existing six
inactive legacy errors, full traceback hashes and protected source/lock hashes.
Applying that baseline to this new two-file release requires explicit owner
scope approval after review; the previous writer release permission does not
grant it. There is no learning, arbitrary baseline flag or silent fallback.
Active DAG errors always block. The existing writer runner and its scope remain
unchanged. A changed baseline requires investigation and a new plan.

## Publication, acceptance and recovery

Only after passing checks, required CI/review/merge and approval of the exact
manifest, window and error policy:

```bash
python3 -B -m deploy.shared_dependencies.release apply \
  --bundle BUNDLE_PATH --approval MANIFEST_SHA256
```

The runner holds the same shared/source flocks as the writer release, using the
actual SofaScore and Transfermarkt state roots. Existing lock bytes are
preserved. It checks source inflight markers and shares
`shared-writer-inflight.json` with the old runner, so an interrupted release
cannot be overtaken. It fetches master and rejects unmerged or advanced payloads.

Each file uses an atomic rename from a durable private file outside watched
directories, preserving owner/mode. **The pair is not one filesystem atomic
operation.** Every active DAG must be paused, current work/drivers drained and
external operators coordinated. Rehearsal checks both forward and reverse
prefixes on all four exact consumer images: 20 fresh networkless processes.
Each process verifies startup with the original lock, imports all active DAG
files, imports affected consumers and exercises the new behavior without live
queries. This is an import/behavior check, not a scheduler or Lakekeeper test.

Acceptance requires fresh scheduler parses of all active DAGs after the final
publication +60 seconds (420-second deadline), the pinned error policy,
unchanged pause/identity/catalog/code closure, and both mounted hashes on every
consumer. Any postflight failure returns nonzero and attempts reverse rollback.
Crash recovery infers exact old/new state independently for both files; foreign
content, permissions or neighbor drift prevents overwriting. No data is reverted.
Cleanup of an already accepted or rolled-back release is outside runtime rollback.
If marker deletion/fsync fails, the command returns nonzero and tries to retain
the marker; it never starts a new, unfenced rollback of verified terminal code.

```bash
# Separately authorized recovery, including containment outside the old window:
python3 -B -m deploy.shared_dependencies.release recover \
  --bundle BUNDLE_PATH --approval MANIFEST_SHA256
```

Recovery still requires the same locks, identity and quiet maintenance state.
Do not remove markers, break locks, copy old lock files or weaken error checks.
If startup compatibility requires a changed image/contract, stop this approach;
use a separately approved code+image ceremony, never a live lock rewrite.

After common acceptance and authorized restoration of the pause map, leave
ClubElo's regular daily enabled and await its normal nightly automatic delivery
at 01:40 UTC / 04:40 МСК. Confirm journal, exact source hashes and scheduler parse.
Do not trigger source delivery manually or repeat the accepted writer release.
See [history and source acceptance](../../docs/operations/clubelo-1465-dependency-acceptance.md).
