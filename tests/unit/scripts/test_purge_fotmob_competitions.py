import hashlib
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from scripts import purge_fotmob_competitions as mod


NOW = datetime(2026, 8, 8, 10, 0, tzinfo=timezone.utc)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _evidence(competition_id: int) -> mod.ProtectedEvidence:
    return mod.ProtectedEvidence(
        competition_id=competition_id,
        catalog_batch_id=f"catalog-{competition_id}",
        evidence_batch_id=f"evidence-{competition_id}",
        profile_target_key=_sha(f"profile-target-{competition_id}"),
        profile_content_hash=_sha(f"profile-content-{competition_id}"),
        catalog_scope_decision="excluded",
        evidence_decision="excluded",
        source_gender="female",
        policy_rule="exclude_female",
        classifier_version="fotmob-men-v1",
        observed_at=NOW - timedelta(days=1),
    )


class FakeBackend:
    def __init__(self, *, journal_path=None):
        self.journal_path = journal_path
        self.active_dags = set()
        self.paused_dags = set(mod.WRITER_DAG_IDS)
        self.lease_active = False
        self.apply_fence = None
        self.evidence = {
            competition_id: _evidence(competition_id)
            for competition_id in mod.PURGE_COMPETITION_IDS
        }
        self.team_ids = tuple(str(value) for value in range(1, 24))
        self.events = []
        self.table_state = {}
        self.protected_survivor_counts = {
            (table, competition_id): 1
            for table in mod.PROTECTED_EVIDENCE_TABLES
            for competition_id in mod.PURGE_COMPETITION_IDS
        }
        for index, table in enumerate(mod.PHASE_A_TABLES):
            protected = 2 if table in mod.PROTECTED_EVIDENCE_TABLES else 0
            self.table_state[table] = mod.TableInspection(
                table=table,
                snapshot_id=100 + index,
                total_count=10 + protected,
                candidate_count=10,
            )
        self.snapshot_ids = {
            table: {inspection.snapshot_id}
            for table, inspection in self.table_state.items()
        }
        self.global_state = {
            table: mod.TableInspection(
                table=table,
                snapshot_id=700 + index,
                total_count=23 + index,
                candidate_count=0,
            )
            for index, table in enumerate(mod.GLOBAL_PRESERVE_TABLES)
        }

        exclusive_hash = _sha("exclusive-body")
        historical_exclusive_hash = _sha("historical-exclusive-body")
        shared_hash = _sha("shared-body")
        self.exclusive_target = _sha("exclusive-target")
        self.shared_target = _sha("shared-target")
        self.all_leagues_target = _sha("all-leagues-target")
        self.transfers_target = _sha("transfers-target")
        self.profile_target = self.evidence[10557].profile_target_key
        self.logical_completion_target = _sha("logical-scope-completion")
        self.manifest_refs = (
            mod.ManifestReference(
                target_key=self.exclusive_target,
                content_hash=exclusive_hash,
                target_type="league_season",
                competition_id=10557,
                batch_id="doomed-a",
                raw_uri=(
                    f"memory://raw/blobs/sha256/{exclusive_hash[:2]}/"
                    f"{exclusive_hash}.json.gz"
                ),
            ),
            mod.ManifestReference(
                target_key=self.exclusive_target,
                content_hash=historical_exclusive_hash,
                target_type="league_season",
                competition_id=10557,
                batch_id="doomed-a-history",
                raw_uri=(
                    f"memory://raw/blobs/sha256/{historical_exclusive_hash[:2]}/"
                    f"{historical_exclusive_hash}.json.gz"
                ),
            ),
            mod.ManifestReference(
                target_key=self.shared_target,
                content_hash=shared_hash,
                target_type="match",
                competition_id=10558,
                batch_id="doomed-b",
                raw_uri=(
                    f"memory://raw/blobs/sha256/{shared_hash[:2]}/"
                    f"{shared_hash}.json.gz"
                ),
            ),
            mod.ManifestReference(
                target_key=self.all_leagues_target,
                content_hash=shared_hash,
                target_type="all_leagues",
                competition_id=None,
                batch_id="shared-catalog",
                raw_uri=(
                    f"memory://raw/blobs/sha256/{shared_hash[:2]}/"
                    f"{shared_hash}.json.gz"
                ),
            ),
            mod.ManifestReference(
                target_key=self.transfers_target,
                content_hash=shared_hash,
                target_type="transfers_page",
                competition_id=None,
                batch_id="shared-transfers",
                raw_uri=(
                    f"memory://raw/blobs/sha256/{shared_hash[:2]}/"
                    f"{shared_hash}.json.gz"
                ),
            ),
            mod.ManifestReference(
                target_key=self.profile_target,
                content_hash=self.evidence[10557].profile_content_hash,
                target_type="competition_profile",
                competition_id=10557,
                batch_id=self.evidence[10557].evidence_batch_id,
                raw_uri=(
                    "memory://raw/blobs/sha256/"
                    f"{self.evidence[10557].profile_content_hash[:2]}/"
                    f"{self.evidence[10557].profile_content_hash}.json.gz"
                ),
            ),
            mod.ManifestReference(
                target_key=self.evidence[10558].profile_target_key,
                content_hash=self.evidence[10558].profile_content_hash,
                target_type="competition_profile",
                competition_id=10558,
                batch_id=self.evidence[10558].evidence_batch_id,
                raw_uri=(
                    "memory://raw/blobs/sha256/"
                    f"{self.evidence[10558].profile_content_hash[:2]}/"
                    f"{self.evidence[10558].profile_content_hash}.json.gz"
                ),
            ),
            mod.ManifestReference(
                target_key=self.logical_completion_target,
                content_hash=_sha("scope-completion-coverage"),
                target_type="scope_completion",
                competition_id=10557,
                batch_id="doomed-logical-completion",
                raw_uri=None,
            ),
        )
        self.raw_targets = {}
        for reference in self.manifest_refs:
            if reference.raw_uri is None:
                continue
            if reference.target_key in self.raw_targets:
                continue
            self.raw_targets[reference.target_key] = mod.RawTargetObject(
                target_key=reference.target_key,
                manifest_path=(
                    f"targets/sha256/{reference.target_key[:2]}/"
                    f"{reference.target_key}.json"
                ),
                manifest_sha256=_sha(f"manifest-{reference.target_key}"),
                content_hash=reference.content_hash,
                blob_path=(
                    f"blobs/sha256/{reference.content_hash[:2]}/"
                    f"{reference.content_hash}.json.gz"
                ),
                blob_sha256=_sha(f"blob-{reference.content_hash}"),
                canonical_url=f"https://www.fotmob.com/api/{reference.target_type}",
            )
        self.raw_exists = {
            obj.manifest_path: obj.manifest_sha256 for obj in self.raw_targets.values()
        }
        self.raw_exists.update(
            {obj.blob_path: obj.blob_sha256 for obj in self.raw_targets.values()}
        )
        historical_path = (
            f"blobs/sha256/{historical_exclusive_hash[:2]}/"
            f"{historical_exclusive_hash}.json.gz"
        )
        self.raw_exists[historical_path] = _sha(
            f"blob-{historical_exclusive_hash}"
        )

    def assert_quiescent(self, writer_dag_ids, *, source):
        self.events.append(("quiescence", tuple(writer_dag_ids), source))
        unpaused = set(writer_dag_ids) - self.paused_dags
        if unpaused:
            raise mod.PurgeRefused(f"unpaused DAGs: {sorted(unpaused)}")
        if self.active_dags:
            raise mod.PurgeRefused(f"active writer DAGs: {sorted(self.active_dags)}")
        if self.lease_active and self.apply_fence is None:
            raise mod.PurgeRefused("active FotMob publication lease")

    def acquire_apply_fence(self, plan_sha256, fence_generation_id):
        assert len(plan_sha256) == 64
        expected = fence_generation_id
        if self.apply_fence is not None:
            assert self.apply_fence == expected
            self.events.append(("reacquire-fence", self.apply_fence))
            self.assert_quiescent(mod.WRITER_DAG_IDS, source="fotmob")
            return self.apply_fence
        if self.lease_active and self.apply_fence is None:
            raise mod.PurgeRefused("active FotMob publication lease")
        self.apply_fence = expected
        self.lease_active = True
        self.events.append(("acquire-fence", self.apply_fence))
        try:
            self.assert_quiescent(mod.WRITER_DAG_IDS, source="fotmob")
        except Exception:
            self.release_apply_fence(self.apply_fence)
            raise
        return self.apply_fence

    def recover_apply_fence(self, plan_sha256, fence_generation_id):
        assert len(plan_sha256) == 64
        if fence_generation_id is None or self.apply_fence is None:
            return None
        expected = fence_generation_id
        if self.apply_fence != expected:
            raise mod.PurgeRefused("purge fence differs")
        self.events.append(("recover-fence", self.apply_fence))
        self.assert_quiescent(mod.WRITER_DAG_IDS, source="fotmob")
        return self.apply_fence

    def release_apply_fence(self, fence_token):
        assert fence_token == self.apply_fence
        self.events.append(("release-fence", fence_token))
        self.apply_fence = None
        self.lease_active = False

    def load_protected_evidence(self, competition_ids):
        self.events.append(("evidence", tuple(competition_ids)))
        return dict(self.evidence)

    def load_global_team_ids(self, competition_ids):
        return self.team_ids

    def inspect_table(self, table, predicate):
        self.events.append(("inspect", table, predicate))
        return self.table_state[table]

    def count_matching_rows(self, table, predicate):
        matches = [
            competition_id
            for competition_id in mod.PURGE_COMPETITION_IDS
            if f"'{competition_id}'" in predicate
        ]
        assert len(matches) == 1
        return self.protected_survivor_counts[(table, matches[0])]

    def inspect_global_tables(self, team_ids):
        assert tuple(team_ids) == self.team_ids
        return dict(self.global_state)

    def load_snapshot_ids(self, table):
        return tuple(sorted(self.snapshot_ids[table]))

    def load_manifest_references(self):
        return tuple(self.manifest_refs)

    def load_raw_targets(self):
        return dict(self.raw_targets)

    def invalidate_raw_inventory(self):
        self.events.append(("invalidate-raw",))

    def validate_raw_target(self, target_key):
        raw = self.raw_targets[target_key]
        if raw.manifest_path not in self.raw_exists or raw.blob_path not in self.raw_exists:
            raise mod.PurgeRefused("raw target/blob integrity")

    def validate_raw_blob(self, blob_path, content_hash):
        assert blob_path.endswith(f"/{content_hash}.json.gz")
        if blob_path not in self.raw_exists:
            raise mod.PurgeRefused("raw blob integrity")

    def delete_table(self, operation):
        assert self.apply_fence is not None
        before = self.table_state[operation.table]
        assert before.snapshot_id == operation.snapshot_id
        assert before.candidate_count == operation.candidate_count
        after = mod.TableInspection(
            table=operation.table,
            snapshot_id=before.snapshot_id + 1000,
            total_count=before.total_count - before.candidate_count,
            candidate_count=0,
        )
        self.table_state[operation.table] = after
        self.snapshot_ids[operation.table].add(after.snapshot_id)
        query_id = f"query-{operation.table}"
        self.events.append(("delete-table", operation.table, operation.predicate))
        return mod.DeleteReceipt(
            table=operation.table,
            parent_snapshot_id=before.snapshot_id,
            snapshot_id=after.snapshot_id,
            operation="delete",
            query_id=query_id,
            snapshot_query_id=query_id,
            deleted_count=before.candidate_count,
        )

    def recover_delete(self, operation):
        current = self.table_state[operation.table]
        if current.snapshot_id == operation.snapshot_id:
            return None
        if current.snapshot_id != operation.snapshot_id + 1000:
            raise mod.PostDeleteVerificationError("in-flight DELETE ownership")
        query_id = f"query-{operation.table}"
        return mod.DeleteReceipt(
            table=operation.table,
            parent_snapshot_id=operation.snapshot_id,
            snapshot_id=current.snapshot_id,
            operation="delete",
            query_id=query_id,
            snapshot_query_id=query_id,
            deleted_count=operation.candidate_count,
        )

    def raw_object_sha256(self, path):
        return self.raw_exists.get(path)

    def raw_blob_ref_count(self, blob_path):
        target_refs = sum(
            obj.blob_path == blob_path and obj.manifest_path in self.raw_exists
            for obj in self.raw_targets.values()
        )
        content_hash = blob_path.split("/")[-1].removesuffix(".json.gz")
        bronze_refs = sum(
            reference.content_hash == content_hash
            and not mod._manifest_reference_is_doomed(
                reference, tuple(self.evidence.values())
            )
            for reference in self.manifest_refs
        )
        return target_refs + bronze_refs

    def delete_raw_object(self, path, expected_sha256):
        assert self.apply_fence is not None
        assert self.raw_exists.get(path) == expected_sha256
        if self.journal_path is not None:
            journal = json.loads(self.journal_path.read_text())
            assert journal["phase_a_validated"] is True
        self.events.append(("delete-raw", path))
        del self.raw_exists[path]

    def expire_snapshots(self, table, snapshot_ids, *, current_snapshot_id):
        assert self.apply_fence is not None
        assert self.table_state[table].snapshot_id == current_snapshot_id
        if self.journal_path is not None:
            journal = json.loads(self.journal_path.read_text())
            assert journal["phase_a_validated"] is True
        assert self.snapshot_ids[table] == {*snapshot_ids, current_snapshot_id}
        self.snapshot_ids[table] = {current_snapshot_id}
        self.events.append(("expire", table, tuple(snapshot_ids)))


def _plan(backend=None):
    return mod.build_plan(
        backend or FakeBackend(),
        now=NOW,
        ttl=timedelta(hours=1),
    )


def test_parser_defaults_to_read_only_plan_mode():
    args = mod.build_parser().parse_args([])
    assert args.apply is False
    assert args.plan is None
    assert args.plan_sha256 is None


@pytest.mark.parametrize("active", ["dag_orchestrate_fotmob", "dag_refresh_fotmob"])
def test_plan_refuses_any_active_new_or_legacy_writer(active):
    backend = FakeBackend()
    backend.active_dags.add(active)
    with pytest.raises(mod.PurgeRefused, match="active writer DAG"):
        _plan(backend)


@pytest.mark.parametrize(
    "dag_id",
    [
        "dag_orchestrate_fotmob",
        "dag_ingest_fotmob",
        "dag_transform_fotmob_silver",
        "dag_iceberg_maintenance",
        "dag_iceberg_maintenance_daily",
    ],
)
def test_plan_requires_every_writer_and_maintenance_dag_paused(dag_id):
    backend = FakeBackend()
    backend.paused_dags.remove(dag_id)
    with pytest.raises(mod.PurgeRefused, match="unpaused DAG"):
        _plan(backend)


def test_plan_refuses_active_publication_lease_and_nonstructural_evidence():
    backend = FakeBackend()
    backend.lease_active = True
    with pytest.raises(mod.PurgeRefused, match="publication lease"):
        _plan(backend)

    backend = FakeBackend()
    backend.evidence[10558] = replace(
        backend.evidence[10558], source_gender="male"
    )
    with pytest.raises(mod.PurgeRefused, match="structural-female"):
        _plan(backend)


def test_plan_refuses_a_missing_protected_profile_blob():
    backend = FakeBackend()
    protected = backend.raw_targets[backend.profile_target]
    del backend.raw_exists[protected.blob_path]
    with pytest.raises(mod.PurgeRefused, match="integrity|protected blob SHA-256"):
        _plan(backend)


def test_production_raw_inventory_reuses_full_manifest_and_blob_validation(tmp_path):
    from scrapers.fotmob.raw_store import FotMobRawStore
    from scrapers.fotmob.transport import canonicalize_target

    store = FotMobRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target("leagues", {"id": 10557})
    record = store.store(target, b'{"id":10557,"gender":"female"}')
    backend = mod.TrinoAirflowRawBackend(object(), store)

    loaded = backend.load_raw_targets()[target.target_key]
    assert loaded.content_hash == record.content_hash
    assert loaded.blob_sha256 == ""
    backend.validate_raw_target(target.target_key)

    manifest_path = tmp_path / store._manifest_key(target.target_key)
    payload = json.loads(manifest_path.read_text())
    payload["canonical_url"] = "https://www.fotmob.com/api/leagues?id=10558"
    manifest_path.write_text(json.dumps(payload))
    backend = mod.TrinoAirflowRawBackend(object(), store)
    with pytest.raises(mod.PurgeRefused, match="identity drifted|integrity is unproven"):
        backend.validate_raw_target(target.target_key)


def test_production_target_delete_refreshes_cache_after_committed_error(tmp_path):
    from scrapers.fotmob.raw_store import FotMobRawStore
    from scrapers.fotmob.transport import canonicalize_target

    store = FotMobRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target("leagues", {"id": 10557})
    store.store(target, b'{"id":10557,"gender":"female"}')
    backend = mod.TrinoAirflowRawBackend(object(), store)
    indexed = backend.load_raw_targets()[target.target_key]

    class CommitThenRaiseFilesystem:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

        def delete_file(self, path):
            self.wrapped.delete_file(path)
            raise RuntimeError("lost storage acknowledgement")

    store.filesystem = CommitThenRaiseFilesystem(store.filesystem)
    with pytest.raises(mod.PostDeleteVerificationError, match="outcome is unknown"):
        backend.delete_raw_object(indexed.manifest_path, indexed.manifest_sha256)

    assert backend.raw_object_sha256(indexed.manifest_path) is None
    assert target.target_key not in backend.load_raw_targets()


def test_plan_is_exact_id_predicated_and_preserves_shared_and_global_objects():
    backend = FakeBackend()
    plan = _plan(backend)

    assert plan["competition_ids"] == [10557, 10558]
    assert set(item["table"] for item in plan["phase_a"]["tables"]) == set(
        mod.PHASE_A_TABLES
    )
    assert set(mod.GLOBAL_PRESERVE_TABLES).isdisjoint(
        item["table"] for item in plan["phase_a"]["tables"]
    )
    assert "fotmob_transfer_events" in plan["global_preservation"]["tables"]
    for operation in plan["phase_a"]["tables"]:
        assert "10557" in operation["predicate"]
        assert "10558" in operation["predicate"]
        assert "run_id" not in operation["predicate"].casefold()
        assert "AS BIGINT" not in operation["predicate"]
        assert "'10557.0'" in operation["predicate"]
        assert "'10558.0'" in operation["predicate"]
    protected_operations = {
        item["table"]: item["predicate"] for item in plan["phase_a"]["tables"]
    }
    assert "_target_batch_id IS NULL OR" in protected_operations[
        "fotmob_competitions"
    ]
    assert "target_key NOT IN" not in protected_operations["fotmob_ingest_manifest"]
    assert "NOT IN ('catalog-10557', 'catalog-10558')" not in protected_operations[
        "fotmob_competitions"
    ]
    assert (
        "NOT IN ('evidence-10557', 'evidence-10558')"
        not in protected_operations["fotmob_competition_scope_observations"]
    )
    assert (
        "NOT IN ('catalog-10557', 'catalog-10558', 'evidence-10557', "
        "'evidence-10558')"
        not in protected_operations["fotmob_ingest_manifest"]
    )
    protected = {
        item["competition_id"]: item for item in plan["protected_evidence"]
    }
    assert protected[10557]["catalog_batch_id"] == "catalog-10557"
    assert protected[10557]["evidence_batch_id"] == "evidence-10557"
    assert plan["global_preservation"]["team_ids"] == list(backend.team_ids)
    assert len(plan["global_preservation"]["team_ids"]) == 23

    phase_b = plan["phase_b"]
    assert [item["target_key"] for item in phase_b["target_manifests"]] == sorted(
        [backend.exclusive_target, backend.shared_target]
    )
    exclusive_blob_paths = {
        (
            f"blobs/sha256/{reference.content_hash[:2]}/"
            f"{reference.content_hash}.json.gz"
        )
        for reference in backend.manifest_refs
        if reference.target_key == backend.exclusive_target
    }
    assert {item["path"] for item in phase_b["blobs"]} == exclusive_blob_paths
    assert len(phase_b["blobs"]) == 2
    shared_blob_path = backend.raw_targets[backend.shared_target].blob_path
    assert shared_blob_path not in {item["path"] for item in phase_b["blobs"]}
    assert shared_blob_path in {
        item["path"] for item in phase_b["shared_blob_exclusions"]
    }
    excluded_keys = {item["target_key"] for item in phase_b["shared_exclusions"]}
    assert {
        backend.all_leagues_target,
        backend.transfers_target,
        backend.profile_target,
    } <= excluded_keys
    assert backend.logical_completion_target not in {
        item["target_key"] for item in phase_b["target_manifests"]
    }


def test_plan_refuses_extra_physical_current_evidence_survivor():
    backend = FakeBackend()
    table = "fotmob_competitions"
    backend.protected_survivor_counts[(table, 10557)] = 2
    with pytest.raises(mod.PurgeRefused, match="exactly one physical evidence row"):
        _plan(backend)


def test_raw_ownership_does_not_preserve_another_ids_protected_batch():
    backend = FakeBackend()
    cross_id = mod.ManifestReference(
        target_key=_sha("cross-id-target"),
        content_hash=_sha("cross-id-body"),
        target_type="competition_profile",
        competition_id=10557,
        batch_id=backend.evidence[10558].evidence_batch_id,
        raw_uri=(
            "memory://raw/blobs/sha256/"
            f"{_sha('cross-id-body')[:2]}/{_sha('cross-id-body')}.json.gz"
        ),
    )
    own_id = replace(cross_id, batch_id=backend.evidence[10557].evidence_batch_id)
    evidence = tuple(backend.evidence.values())
    assert mod._manifest_reference_is_doomed(cross_id, evidence) is True
    assert mod._manifest_reference_is_doomed(own_id, evidence) is False


def test_apply_requires_exact_unexpired_canonical_hash_before_any_write(tmp_path):
    backend = FakeBackend()
    plan = _plan(backend)
    journal = tmp_path / "journal.json"

    with pytest.raises(mod.PurgeRefused, match="supplied plan SHA"):
        mod.apply_plan(
            plan,
            backend,
            supplied_sha256="0" * 64,
            journal_path=journal,
            now=NOW,
        )
    assert not any(event[0].startswith("delete") for event in backend.events)

    expired = dict(plan)
    expired["created_at"] = (NOW - timedelta(hours=2)).isoformat()
    expired["expires_at"] = (NOW - timedelta(hours=1)).isoformat()
    expired = mod.with_plan_hash(expired)
    with pytest.raises(mod.PurgeRefused, match="expired"):
        mod.apply_plan(
            expired,
            backend,
            supplied_sha256=expired["plan_sha256"],
            journal_path=journal,
            now=NOW,
        )


def test_phase_a_snapshot_ownership_failure_blocks_raw_and_expiration(tmp_path):
    backend = FakeBackend()
    plan = _plan(backend)
    original = backend.delete_table

    def wrong_owner(operation):
        receipt = original(operation)
        return replace(receipt, snapshot_query_id="some-other-query")

    backend.delete_table = wrong_owner
    with pytest.raises(mod.PostDeleteVerificationError, match="snapshot ownership"):
        mod.apply_plan(
            plan,
            backend,
            supplied_sha256=plan["plan_sha256"],
            journal_path=tmp_path / "journal.json",
            now=NOW,
        )
    assert not any(event[0] in {"delete-raw", "expire"} for event in backend.events)


def test_apply_revalidates_drift_then_runs_durable_phase_a_before_phase_b(tmp_path):
    journal = tmp_path / "journal.json"
    backend = FakeBackend(journal_path=journal)
    plan = _plan(backend)
    result = mod.apply_plan(
        plan,
        backend,
        supplied_sha256=plan["plan_sha256"],
        journal_path=journal,
        now=NOW,
    )

    assert result["status"] == "complete"
    persisted = json.loads(journal.read_text())
    assert persisted["phase_a_validated"] is True
    assert persisted["status"] == "complete"
    assert len(persisted["phase_a_receipts"]) == len(mod.PHASE_A_TABLES)
    raw_events = [event for event in backend.events if event[0] == "delete-raw"]
    assert {event[1] for event in raw_events} == {
        item["path"] for item in plan["phase_b"]["target_manifests"]
    } | {item["path"] for item in plan["phase_b"]["blobs"]}
    assert not any("all_leagues" in event[1] for event in raw_events)
    assert len([event for event in backend.events if event[0] == "expire"]) == len(
        mod.PHASE_A_TABLES
    )
    mutation_indexes = [
        index
        for index, event in enumerate(backend.events)
        if event[0] in {"delete-table", "delete-raw", "expire"}
    ]
    assert backend.events[mutation_indexes[0] - 1][0] != "release-fence"
    assert next(
        index for index, event in enumerate(backend.events) if event[0] == "acquire-fence"
    ) < mutation_indexes[0]
    assert next(
        index for index, event in enumerate(backend.events) if event[0] == "release-fence"
    ) > mutation_indexes[-1]
    assert backend.apply_fence is None


def test_resume_recovers_noop_prefix_and_committed_inflight_delete(tmp_path):
    journal = tmp_path / "journal.json"
    backend = FakeBackend(journal_path=journal)
    first_table = mod.PHASE_A_TABLES[0]
    backend.table_state[first_table] = replace(
        backend.table_state[first_table], candidate_count=0
    )
    plan = _plan(backend)
    original = backend.delete_table
    crashed = False

    def commit_then_crash(operation):
        nonlocal crashed
        receipt = original(operation)
        if not crashed:
            crashed = True
            raise RuntimeError("lost process after committed DELETE")
        return receipt

    backend.delete_table = commit_then_crash
    with pytest.raises(mod.PostDeleteVerificationError, match="outcome is unknown"):
        mod.apply_plan(
            plan,
            backend,
            supplied_sha256=plan["plan_sha256"],
            journal_path=journal,
            now=NOW,
        )
    persisted = json.loads(journal.read_text())
    assert persisted["phase_a_receipts"][first_table]["operation"] == "noop"
    assert persisted["phase_a_intent"]["table"] == mod.PHASE_A_TABLES[1]

    backend.delete_table = original
    result = mod.apply_plan(
        plan,
        backend,
        supplied_sha256=plan["plan_sha256"],
        journal_path=journal,
        now=NOW,
    )
    assert result["status"] == "complete"
    assert result["phase_a_intent"] is None


def test_committed_delete_resume_remains_recoverable_after_plan_expiry(tmp_path):
    journal = tmp_path / "journal.json"
    backend = FakeBackend(journal_path=journal)
    plan = _plan(backend)
    original = backend.delete_table
    crashed = False

    def commit_then_crash(operation):
        nonlocal crashed
        receipt = original(operation)
        if not crashed:
            crashed = True
            raise RuntimeError("lost process after committed DELETE")
        return receipt

    backend.delete_table = commit_then_crash
    with pytest.raises(mod.PostDeleteVerificationError):
        mod.apply_plan(
            plan,
            backend,
            supplied_sha256=plan["plan_sha256"],
            journal_path=journal,
            now=NOW,
        )
    backend.delete_table = original
    result = mod.apply_plan(
        plan,
        backend,
        supplied_sha256=plan["plan_sha256"],
        journal_path=journal,
        now=NOW + timedelta(hours=2),
    )
    assert result["status"] == "complete"


def test_complete_journal_recovers_and_releases_exact_fence_after_crash(tmp_path):
    journal = tmp_path / "journal.json"
    backend = FakeBackend(journal_path=journal)
    plan = _plan(backend)
    original = backend.release_apply_fence

    def crash_before_release(_token):
        raise RuntimeError("lost process before fence release")

    backend.release_apply_fence = crash_before_release
    with pytest.raises(RuntimeError, match="before fence release"):
        mod.apply_plan(
            plan,
            backend,
            supplied_sha256=plan["plan_sha256"],
            journal_path=journal,
            now=NOW,
        )
    assert json.loads(journal.read_text())["status"] == "complete"
    assert backend.apply_fence is not None

    backend.release_apply_fence = original
    result = mod.apply_plan(
        plan,
        backend,
        supplied_sha256=plan["plan_sha256"],
        journal_path=journal,
        now=NOW,
    )
    assert result["status"] == "complete"
    assert backend.apply_fence is None
    assert any(event[0] == "recover-fence" for event in backend.events)


def test_corrupt_complete_journal_cannot_execute_missing_phase_b_work(tmp_path):
    journal = tmp_path / "journal.json"
    backend = FakeBackend(journal_path=journal)
    plan = _plan(backend)
    result = mod.apply_plan(
        plan,
        backend,
        supplied_sha256=plan["plan_sha256"],
        journal_path=journal,
        now=NOW,
    )
    result["deleted_raw_objects"] = []
    journal.write_text(json.dumps(result))
    prior_mutations = len(
        [
            event
            for event in backend.events
            if event[0] in {"delete-table", "delete-raw", "expire"}
        ]
    )
    with pytest.raises(mod.PurgeRefused, match="complete purge journal"):
        mod.apply_plan(
            plan,
            backend,
            supplied_sha256=plan["plan_sha256"],
            journal_path=journal,
            now=NOW,
        )
    assert len(
        [
            event
            for event in backend.events
            if event[0] in {"delete-table", "delete-raw", "expire"}
        ]
    ) == prior_mutations


def test_post_fence_prestate_drift_releases_before_any_mutation(tmp_path):
    backend = FakeBackend()
    plan = _plan(backend)
    original = backend.inspect_table

    def drift_only_after_fence(table, predicate):
        observed = original(table, predicate)
        if backend.apply_fence is not None and table == mod.PHASE_A_TABLES[0]:
            return replace(observed, total_count=observed.total_count + 1)
        return observed

    backend.inspect_table = drift_only_after_fence
    with pytest.raises(
        mod.PurgeRefused,
        match="drifted after fence acquisition|post-fence pre-apply drift",
    ):
        mod.apply_plan(
            plan,
            backend,
            supplied_sha256=plan["plan_sha256"],
            journal_path=tmp_path / "journal.json",
            now=NOW,
        )
    assert backend.apply_fence is None
    assert not any(event[0] == "delete-table" for event in backend.events)


def test_empty_journal_recovers_crash_immediately_after_fence_acquisition(tmp_path):
    journal = tmp_path / "journal.json"
    backend = FakeBackend(journal_path=journal)
    plan = _plan(backend)
    original = backend.inspect_table
    crashed = False

    def process_dies_after_fence(table, predicate):
        nonlocal crashed
        if backend.apply_fence is not None and not crashed:
            crashed = True
            raise SystemExit("synthetic hard crash")
        return original(table, predicate)

    backend.inspect_table = process_dies_after_fence
    with pytest.raises(SystemExit, match="synthetic hard crash"):
        mod.apply_plan(
            plan,
            backend,
            supplied_sha256=plan["plan_sha256"],
            journal_path=journal,
            now=NOW,
        )
    persisted = json.loads(journal.read_text())
    assert persisted["phase_a_receipts"] == {}
    assert persisted["phase_a_intent"] is None
    assert persisted["apply_fence_generation_id"] == backend.apply_fence

    backend.inspect_table = original
    result = mod.apply_plan(
        plan,
        backend,
        supplied_sha256=plan["plan_sha256"],
        journal_path=journal,
        now=NOW,
    )
    assert result["status"] == "complete"
    assert result["apply_fence_generation_id"] is None


def test_recovered_empty_fence_releases_when_live_plan_drifted(tmp_path):
    journal = tmp_path / "journal.json"
    backend = FakeBackend(journal_path=journal)
    plan = _plan(backend)
    original = backend.inspect_table
    crashed = False

    def process_dies_after_fence(table, predicate):
        nonlocal crashed
        if backend.apply_fence is not None and not crashed:
            crashed = True
            raise SystemExit("synthetic hard crash")
        return original(table, predicate)

    backend.inspect_table = process_dies_after_fence
    with pytest.raises(SystemExit, match="synthetic hard crash"):
        mod.apply_plan(
            plan,
            backend,
            supplied_sha256=plan["plan_sha256"],
            journal_path=journal,
            now=NOW,
        )
    assert backend.apply_fence is not None

    backend.inspect_table = original
    backend.protected_survivor_counts[("fotmob_competitions", 10557)] = 0
    with pytest.raises(mod.PurgeRefused, match="exactly one physical evidence row"):
        mod.apply_plan(
            plan,
            backend,
            supplied_sha256=plan["plan_sha256"],
            journal_path=journal,
            now=NOW,
        )

    persisted = json.loads(journal.read_text())
    assert persisted["apply_fence_generation_id"] is None
    assert backend.apply_fence is None
    assert any(event[0] == "recover-fence" for event in backend.events)
    assert not any(event[0] == "delete-table" for event in backend.events)


@pytest.mark.parametrize("crash_point", ["snapshot", "raw"])
def test_resume_recovers_committed_phase_b_mutation_from_write_ahead_intent(
    tmp_path, crash_point
):
    journal = tmp_path / "journal.json"
    backend = FakeBackend(journal_path=journal)
    plan = _plan(backend)
    if crash_point == "snapshot":
        original = backend.expire_snapshots
    else:
        original = backend.delete_raw_object
    crashed = False

    def commit_then_crash(*args, **kwargs):
        nonlocal crashed
        result = original(*args, **kwargs)
        if not crashed:
            crashed = True
            raise RuntimeError(f"lost process after committed {crash_point}")
        return result

    if crash_point == "snapshot":
        backend.expire_snapshots = commit_then_crash
    else:
        backend.delete_raw_object = commit_then_crash
    with pytest.raises(
        mod.PostDeleteVerificationError,
        match="snapshot expiration outcome|raw target deletion outcome",
    ):
        mod.apply_plan(
            plan,
            backend,
            supplied_sha256=plan["plan_sha256"],
            journal_path=journal,
            now=NOW,
        )

    if crash_point == "snapshot":
        backend.expire_snapshots = original
    else:
        backend.delete_raw_object = original
    result = mod.apply_plan(
        plan,
        backend,
        supplied_sha256=plan["plan_sha256"],
        journal_path=journal,
        now=NOW,
    )
    assert result["status"] == "complete"
    assert result["snapshot_expiration_intent"] is None
    assert result["raw_delete_intent"] is None


def test_apply_rejects_count_drift_before_first_delete(tmp_path):
    backend = FakeBackend()
    plan = _plan(backend)
    table = mod.PHASE_A_TABLES[0]
    backend.table_state[table] = replace(
        backend.table_state[table], candidate_count=11
    )
    with pytest.raises(mod.PurgeRefused, match="drift"):
        mod.apply_plan(
            plan,
            backend,
            supplied_sha256=plan["plan_sha256"],
            journal_path=tmp_path / "journal.json",
            now=NOW,
        )
    assert not any(event[0] == "delete-table" for event in backend.events)


def test_apply_reconstructs_raw_reachability_before_first_delete(tmp_path):
    backend = FakeBackend()
    plan = _plan(backend)
    forged = json.loads(json.dumps(plan))
    forged["phase_b"]["target_manifests"] = []
    forged = mod.with_plan_hash(forged)
    with pytest.raises(mod.PurgeRefused, match="phase_b.*live reconstruction"):
        mod.apply_plan(
            forged,
            backend,
            supplied_sha256=forged["plan_sha256"],
            journal_path=tmp_path / "journal.json",
            now=NOW,
        )
    assert not any(event[0] == "delete-table" for event in backend.events)


def test_resume_is_bound_to_the_exact_plan_hash(tmp_path):
    backend = FakeBackend()
    plan = _plan(backend)
    journal = tmp_path / "journal.json"
    journal.write_text(
        json.dumps(
            {
                "schema_version": mod.JOURNAL_SCHEMA_VERSION,
                "plan_sha256": "f" * 64,
                "status": "phase_a",
                "phase_a_validated": False,
                "phase_a_receipts": {},
                "phase_a_intent": None,
                "apply_fence_generation_id": None,
                "deleted_raw_objects": [],
                "raw_delete_intent": None,
                "expired_snapshots": [],
                "snapshot_expiration_intent": None,
            }
        )
    )
    with pytest.raises(mod.PurgeRefused, match="journal.*different plan"):
        mod.apply_plan(
            plan,
            backend,
            supplied_sha256=plan["plan_sha256"],
            journal_path=journal,
            now=NOW,
        )


def test_journal_cannot_skip_live_reconstruction_with_an_extraneous_receipt(
    tmp_path,
):
    backend = FakeBackend()
    plan = _plan(backend)
    journal = tmp_path / "journal.json"
    journal.write_text(
        json.dumps(
            {
                "schema_version": mod.JOURNAL_SCHEMA_VERSION,
                "plan_sha256": plan["plan_sha256"],
                "status": "phase_a",
                "phase_a_validated": False,
                "phase_a_receipts": {"x": {}},
                "phase_a_intent": None,
                "apply_fence_generation_id": None,
                "deleted_raw_objects": [],
                "raw_delete_intent": None,
                "expired_snapshots": [],
                "snapshot_expiration_intent": None,
            }
        )
    )
    with pytest.raises(mod.PurgeRefused, match="exact Phase A prefix"):
        mod.apply_plan(
            plan,
            backend,
            supplied_sha256=plan["plan_sha256"],
            journal_path=journal,
            now=NOW,
        )
    assert not any(event[0] == "delete-table" for event in backend.events)


def test_cli_dry_run_writes_plan_without_mutation(tmp_path):
    backend = FakeBackend()
    output = tmp_path / "plan.json"
    code = mod.main(
        ["--output", str(output)],
        backend_factory=lambda _args: backend,
    )
    assert code == 0
    assert json.loads(output.read_text())["competition_ids"] == [10557, 10558]
    assert not any(event[0].startswith("delete") for event in backend.events)


@pytest.mark.parametrize("failure", ["wrong_hash", "expired"])
def test_cli_rejects_invalid_plan_before_constructing_writer_backend(
    tmp_path, failure
):
    plan = _plan()
    supplied = plan["plan_sha256"]
    if failure == "wrong_hash":
        supplied = "0" * 64
    else:
        clock = datetime.now(timezone.utc)
        plan["created_at"] = (clock - timedelta(hours=2)).isoformat()
        plan["expires_at"] = (clock - timedelta(hours=1)).isoformat()
        plan = mod.with_plan_hash(plan)
        supplied = plan["plan_sha256"]
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    called = False

    def factory(_args):
        nonlocal called
        called = True
        raise AssertionError("writer backend must not be constructed")

    code = mod.main(
        [
            "--apply",
            "--plan",
            str(plan_path),
            "--plan-sha256",
            supplied,
            "--journal",
            str(tmp_path / "journal.json"),
        ],
        backend_factory=factory,
    )
    assert code == 2
    assert called is False


def test_cli_expired_empty_journal_recovers_and_releases_journaled_fence(tmp_path):
    backend = FakeBackend()
    plan = _plan(backend)
    clock = datetime.now(timezone.utc)
    plan["created_at"] = (clock - timedelta(hours=2)).isoformat()
    plan["expires_at"] = (clock - timedelta(hours=1)).isoformat()
    plan = mod.with_plan_hash(plan)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    journal_path = tmp_path / "journal.json"
    generation_id = "00000000-0000-4000-8000-000000000001"
    journal = mod._initial_journal(plan["plan_sha256"])
    journal["apply_fence_generation_id"] = generation_id
    journal_path.write_text(json.dumps(journal))
    backend.apply_fence = generation_id
    backend.lease_active = True
    called = False

    def factory(_args):
        nonlocal called
        called = True
        return backend

    code = mod.main(
        [
            "--apply",
            "--plan",
            str(plan_path),
            "--plan-sha256",
            plan["plan_sha256"],
            "--journal",
            str(journal_path),
        ],
        backend_factory=factory,
    )
    assert code == 2
    assert called is True
    assert backend.apply_fence is None
    assert json.loads(journal_path.read_text())["apply_fence_generation_id"] is None
