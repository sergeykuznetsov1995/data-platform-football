"""
Unit tests for ``dags/utils/maintenance_tasks.py``.

Covers the small but load-bearing constants that drive the daily Iceberg
maintenance DAG. Without a delete-then-insert table in ``HIGH_CHURN_BRONZE``
it accumulates daily ``INSERT`` snapshots and metadata bloats indefinitely
(see Apr 2026 incident: ~26 GB metadata on whoscored_events).

Pure constant-table tests — no Trino, no Airflow, no DAG load.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


@pytest.mark.unit
class TestHighChurnBronzeAllowlist:
    """Verify the daily-VACUUM allowlist contains every known
    delete-then-insert / replace-partitions table."""

    def test_allowlist_is_a_tuple(self):
        """HIGH_CHURN_BRONZE is declared as a tuple (immutable) so it
        can't be mutated at runtime by accident."""
        from utils.maintenance_tasks import HIGH_CHURN_BRONZE

        assert isinstance(HIGH_CHURN_BRONZE, tuple)
        # Sanity: at least the historically-known offenders are present.
        for table in (
            "whoscored_events",
            "whoscored_lineups",
            "whoscored_schedule",
            "whoscored_match_ingest_manifest",
            "fbref_match_events",
        ):
            assert table in HIGH_CHURN_BRONZE, (
                f"{table!r} disappeared from HIGH_CHURN_BRONZE — "
                "if intentional, update this test."
            )

    def test_allowlist_has_no_duplicates(self):
        """Defensive check: a misordered edit could land the same table
        twice. Trino EXECUTE expire_snapshots is idempotent, but we
        still want config to be clean."""
        from utils.maintenance_tasks import HIGH_CHURN_BRONZE

        assert len(set(HIGH_CHURN_BRONZE)) == len(HIGH_CHURN_BRONZE)

    def test_daily_fotmob_sofascore_espn_writers_listed(self):
        """#266: these daily writers bloated to multi-GB metadata
        (fotmob_match_details hit 7.2 GB / 154 MB data) because they were
        never on the allow-list. They must stay listed."""
        from utils.maintenance_tasks import HIGH_CHURN_BRONZE

        for table in (
            "fotmob_match_details",
            "fotmob_player_details",
            "fotmob_player_stats",
            "sofascore_player_ratings",
            "espn_lineup",
            "espn_matchsheet",
        ):
            assert table in HIGH_CHURN_BRONZE, (
                f"{table!r} must be in HIGH_CHURN_BRONZE (#266)."
            )


@pytest.mark.unit
class TestSessionMinRetention:
    """#266: the daily DAG asks for '3d', shorter than Trino's 7d default
    floor. The per-session override must be strictly shorter than any
    threshold the module uses, or every expire is rejected and the sweep
    no-ops silently."""

    def test_session_floor_below_daily_threshold(self):
        from utils.maintenance_tasks import SESSION_MIN_RETENTION

        # '1h' parsed crudely: must be sub-day so it clears the 3d daily ask.
        assert SESSION_MIN_RETENTION.endswith(("h", "m", "s")), (
            "SESSION_MIN_RETENTION must be sub-day (e.g. '1h') so the "
            "daily '3d' threshold is honored."
        )


@pytest.mark.unit
class TestFBrefGenericStageJanitor:
    REFRESH = "cb02b6ce-aab7-4c9a-85d0-1292a49e03a2"
    STAGE = (
        "fbref_table_cells__stg_"
        "lr_cb02b6ceaab74c9a85d01292a49e03a2_c"
    )
    LEGACY_STAGE = (
        "fbref_table_cells__stg_"
        "cb02b6ce_aab7_4c9a_85d0_1292a49e03a2_deadbeefcafe_c"
    )
    NOW = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)

    @staticmethod
    def _owner(**updates):
        return {
            "run_id": "8ca16a99-4039-44a6-a47d-206037f11e70",
            "run_status": "succeeded",
            "active_fetch_lease": False,
            "active_budget_reservation": False,
            "active_observation_processing": False,
            **updates,
        }

    @staticmethod
    def _run(run_id):
        return {"run_id": run_id, "status": "succeeded"}

    def _wire(
        self,
        monkeypatch,
        stages,
        *,
        age_hours=48,
        delta=0,
        run_ids=None,
        null_owner_rows=0,
    ):
        import utils.maintenance_tasks as maintenance

        resolved_run_ids = tuple(
            run_ids if run_ids is not None else (self._owner()["run_id"],)
        )
        stage_rows = len(resolved_run_ids) + int(null_owner_rows)

        monkeypatch.setattr(maintenance, "_list_tables", lambda *_args: stages)
        monkeypatch.setattr(
            maintenance,
            "_fbref_stage_created_at",
            lambda *_args: self.NOW - timedelta(hours=age_hours),
        )
        monkeypatch.setattr(
            maintenance,
            "_fbref_stage_owner_evidence",
            lambda *_args, **_kwargs: {
                "row_count": stage_rows,
                "null_owner_rows": int(null_owner_rows),
                "run_ids": resolved_run_ids,
            },
        )
        monkeypatch.setattr(
            maintenance,
            "_fbref_stage_semantic_delta",
            lambda *_args: delta,
        )
        monkeypatch.setattr(
            maintenance,
            "_fbref_stage_row_count",
            lambda *_args: stage_rows,
        )
        return maintenance

    @pytest.mark.parametrize("stage", [STAGE, LEGACY_STAGE])
    def test_audit_marks_only_attributable_redundant_terminal_stage_eligible(
        self, monkeypatch, stage
    ):
        maintenance = self._wire(monkeypatch, [stage])

        result = maintenance.janitor_fbref_generic_stages(
            conn=object(),
            owner_lookup=lambda refresh: self._owner()
            if refresh == self.REFRESH
            else None,
            run_lookup=self._run,
            apply=False,
            now=self.NOW,
        )

        assert result["eligible_count"] == 1
        assert result["decisions"][0]["reason"] == "redundant_terminal"

    def test_empty_validated_stage_is_eligible_from_decoded_control_owner(
        self, monkeypatch
    ):
        maintenance = self._wire(monkeypatch, [self.STAGE], run_ids=())

        result = maintenance.janitor_fbref_generic_stages(
            conn=object(),
            owner_lookup=lambda _refresh: self._owner(),
            run_lookup=self._run,
            apply=False,
            now=self.NOW,
        )

        assert result["eligible_count"] == 1
        assert result["decisions"][0]["stage_run_ids"] == []

    def test_age_uses_latest_stage_snapshot(self):
        import utils.maintenance_tasks as maintenance

        class Cursor:
            def execute(self, sql):
                self.sql = sql

            def fetchall(self):
                return [(self.timestamp,)]

            def close(self):
                pass

        cursor = Cursor()
        cursor.timestamp = self.NOW
        connection = type("Connection", (), {"cursor": lambda _self: cursor})()

        assert maintenance._fbref_stage_created_at(
            connection, self.STAGE
        ) == self.NOW
        assert "max(committed_at)" in cursor.sql

    def test_semantic_diff_excludes_operational_lineage_columns(self):
        import utils.maintenance_tasks as maintenance

        class Cursor:
            def execute(self, sql):
                self.sql = sql

            def fetchall(self):
                return [(0,)]

            def close(self):
                pass

        cursor = Cursor()
        connection = type("Connection", (), {"cursor": lambda _self: cursor})()

        assert maintenance._fbref_stage_semantic_delta(
            connection, self.STAGE, "fbref_table_cells"
        ) == 0
        assert "EXCEPT" in cursor.sql
        assert '"content_hash"' in cursor.sql
        assert '"run_id"' not in cursor.sql
        assert '"persisted_at"' not in cursor.sql

    def test_owner_evidence_counts_null_rows_and_has_no_unchecked_limit(self):
        import utils.maintenance_tasks as maintenance

        run_id = self._owner()["run_id"]

        class Cursor:
            def execute(self, sql):
                self.sql = sql

            def fetchall(self):
                return [(None, 2), (run_id, 4)]

            def close(self):
                pass

        cursor = Cursor()
        connection = type("Connection", (), {"cursor": lambda _self: cursor})()

        evidence = maintenance._fbref_stage_owner_evidence(
            connection, self.STAGE
        )

        assert evidence == {
            "row_count": 6,
            "null_owner_rows": 2,
            "run_ids": (run_id,),
        }
        assert "GROUP BY" in cursor.sql
        assert "LIMIT" not in cursor.sql

    def test_apply_rechecks_and_drops_exact_allowlisted_stage(
        self, monkeypatch
    ):
        maintenance = self._wire(monkeypatch, [self.STAGE])
        dropped = []
        monkeypatch.setattr(
            maintenance, "_drop_fbref_stage", lambda _conn, table: dropped.append(table)
        )

        result = maintenance.janitor_fbref_generic_stages(
            conn=object(),
            owner_lookup=lambda _refresh: self._owner(),
            run_lookup=self._run,
            before_drop=lambda _stage, _refresh: None,
            apply=True,
            now=self.NOW,
        )

        assert dropped == [self.STAGE]
        assert result["dropped_count"] == 1

    @pytest.mark.parametrize(
        ("stage", "age", "owner", "delta", "reason"),
        [
            (
                "fbref_table_cells__stg_not_an_owner_c",
                48,
                None,
                0,
                "unrecognized_generic_stage",
            ),
            (STAGE, 2, None, 0, "younger_than_min_age"),
            (STAGE, 48, {"missing": True}, 0, "unknown_control_owner"),
            (
                STAGE,
                48,
                {"run_status": "running"},
                0,
                "owner_run_not_terminal",
            ),
            (
                STAGE,
                48,
                {"active_observation_processing": True},
                0,
                "active_control_state",
            ),
            (STAGE, 48, {}, 7, "semantic_delta_present"),
        ],
    )
    def test_protection_rules_fail_closed(
        self, monkeypatch, stage, age, owner, delta, reason
    ):
        merged_owner = (
            None
            if owner is None or owner.get("missing")
            else self._owner(**owner)
        )
        maintenance = self._wire(
            monkeypatch, [stage], age_hours=age, delta=delta
        )

        result = maintenance.janitor_fbref_generic_stages(
            conn=object(),
            owner_lookup=lambda _refresh: merged_owner,
            run_lookup=self._run,
            before_drop=lambda _stage, _refresh: None,
            apply=True,
            now=self.NOW,
        )

        assert result["protected_count"] == 1
        assert result["decisions"][0]["reason"] == reason
        assert result["attention_required_count"] == (
            0
            if reason in {"younger_than_min_age", "active_control_state"}
            else 1
        )

    def test_replay_processing_run_is_validated_independently_from_source_owner(
        self, monkeypatch
    ):
        replay_run = "e42eeb80-0aa9-42b4-ad0f-ea334698884c"
        maintenance = self._wire(
            monkeypatch, [self.STAGE], run_ids=(replay_run,)
        )

        result = maintenance.janitor_fbref_generic_stages(
            conn=object(),
            owner_lookup=lambda _refresh: self._owner(),
            run_lookup=lambda run_id: {
                "run_id": run_id,
                "status": "succeeded",
            },
            apply=False,
            now=self.NOW,
        )

        assert result["eligible_count"] == 1
        decision = result["decisions"][0]
        assert decision["owner_run_id"] == self._owner()["run_id"]
        assert decision["stage_run_ids"] == [replay_run]
        assert decision["processing_runs"][0]["terminal"] is True

    def test_nonterminal_replay_processing_run_is_protected(
        self, monkeypatch
    ):
        replay_run = "e42eeb80-0aa9-42b4-ad0f-ea334698884c"
        maintenance = self._wire(
            monkeypatch, [self.STAGE], run_ids=(replay_run,)
        )

        result = maintenance.janitor_fbref_generic_stages(
            conn=object(),
            owner_lookup=lambda _refresh: self._owner(),
            run_lookup=lambda run_id: {"run_id": run_id, "status": "running"},
            apply=False,
            now=self.NOW,
        )

        assert result["protected_count"] == 1
        assert result["decisions"][0]["reason"] == (
            "processing_run_not_terminal"
        )

    def test_nonempty_stage_with_null_processing_owner_is_protected(
        self, monkeypatch
    ):
        maintenance = self._wire(
            monkeypatch,
            [self.STAGE],
            run_ids=(),
            null_owner_rows=2,
        )

        result = maintenance.janitor_fbref_generic_stages(
            conn=object(),
            owner_lookup=lambda _refresh: self._owner(),
            run_lookup=self._run,
            before_drop=lambda *_args: pytest.fail(
                "unattributed nonempty stage must never be dropped"
            ),
            apply=True,
            now=self.NOW,
        )

        assert result["protected_count"] == 1
        decision = result["decisions"][0]
        assert decision["stage_row_count"] == 2
        assert decision["null_processing_owner_rows"] == 2
        assert decision["reason"] == "processing_owner_missing"
        assert result["attention_required_count"] == 1

    def test_typed_stage_is_reported_with_recovery_evidence_and_never_dropped(
        self, monkeypatch
    ):
        import utils.maintenance_tasks as maintenance

        typed = (
            "fbref_match_events__stg_"
            "fbref_0123456789abcdef_deadbeefcafe"
        )
        replay_run = "e42eeb80-0aa9-42b4-ad0f-ea334698884c"
        monkeypatch.setattr(maintenance, "_list_tables", lambda *_args: [typed])
        monkeypatch.setattr(
            maintenance,
            "_fbref_stage_created_at",
            lambda *_args: self.NOW - timedelta(hours=48),
        )
        monkeypatch.setattr(
            maintenance,
            "_fbref_stage_owner_evidence",
            lambda *_args, **_kwargs: {
                "row_count": 7,
                "null_owner_rows": 0,
                "run_ids": (replay_run,),
            },
        )
        monkeypatch.setattr(
            maintenance, "_fbref_stage_row_count", lambda *_args: 7
        )
        dropped = []
        monkeypatch.setattr(
            maintenance,
            "_drop_fbref_stage",
            lambda _conn, table: dropped.append(table),
        )

        result = maintenance.janitor_fbref_generic_stages(
            conn=object(),
            owner_lookup=lambda _refresh: None,
            run_lookup=self._run,
            before_drop=lambda _stage, _refresh: pytest.fail(
                "typed stage must not enter destructive path"
            ),
            apply=True,
            now=self.NOW,
        )

        assert dropped == []
        assert result["protected_count"] == 1
        decision = result["decisions"][0]
        assert decision["stage_family"] == "typed"
        assert decision["live_table"] == "fbref_match_events"
        assert decision["stage_run_ids"] == [replay_run]
        assert decision["stage_row_count"] == 7
        assert decision["reason"] == "typed_stage_requires_recovery_review"
        assert result["attention_required_count"] == 1

    def test_publication_and_unknown_stages_are_inventoried_fail_closed(
        self, monkeypatch
    ):
        import utils.maintenance_tasks as maintenance

        scope_stage = (
            "fbref_target_scope__stg_scope_"
            "8ca16a99403944a6a47d206037f11e70"
        )
        unknown_stage = "fbref_future_writer__stg_unattributed_deadbeef"
        monkeypatch.setattr(
            maintenance,
            "_list_tables",
            lambda *_args: [scope_stage, unknown_stage],
        )
        monkeypatch.setattr(
            maintenance,
            "_fbref_stage_created_at",
            lambda *_args: self.NOW - timedelta(hours=48),
        )
        monkeypatch.setattr(
            maintenance, "_fbref_stage_row_count", lambda *_args: 3
        )
        monkeypatch.setattr(
            maintenance,
            "_fbref_stage_owner_evidence",
            lambda *_args, **_kwargs: {
                "row_count": 3,
                "null_owner_rows": 0,
                "run_ids": (self._owner()["run_id"],),
            },
        )

        result = maintenance.janitor_fbref_generic_stages(
            conn=object(),
            owner_lookup=lambda _refresh: None,
            run_lookup=self._run,
            apply=False,
            now=self.NOW,
        )

        decisions = {item["stage"]: item for item in result["decisions"]}
        assert decisions[scope_stage]["stage_family"] == "publication_scope"
        assert decisions[scope_stage]["reason"] == (
            "publication_scope_stage_requires_recovery_review"
        )
        assert decisions[unknown_stage]["stage_family"] == "unknown"
        assert decisions[unknown_stage]["reason"] == "unsupported_fbref_stage"
        assert result["protected_count"] == 2
        assert result["attention_required_count"] == 2

    def test_future_typed_table_suffix_is_classified_without_allowlist_drift(
        self,
    ):
        import utils.maintenance_tasks as maintenance

        family, live = maintenance._fbref_stage_family(
            "fbref_future_dataset__stg_"
            "fbref_0123456789abcdef_deadbeefcafe"
        )

        assert family == "typed"
        assert live == "fbref_future_dataset"

    def test_apply_fences_immediately_before_drop(self, monkeypatch):
        maintenance = self._wire(monkeypatch, [self.STAGE])
        events = []
        monkeypatch.setattr(
            maintenance,
            "_drop_fbref_stage",
            lambda _conn, table: events.append(("drop", table)),
        )

        maintenance.janitor_fbref_generic_stages(
            conn=object(),
            owner_lookup=lambda _refresh: self._owner(),
            run_lookup=self._run,
            before_drop=lambda stage, refresh: events.append(
                ("fence", stage, refresh)
            ),
            apply=True,
            now=self.NOW,
        )

        assert events == [
            ("fence", self.STAGE, self.REFRESH),
            ("drop", self.STAGE),
        ]

    def test_apply_requires_explicit_destructive_fence(self):
        import utils.maintenance_tasks as maintenance

        with pytest.raises(ValueError, match="before_drop fence"):
            maintenance.janitor_fbref_generic_stages(
                conn=object(),
                owner_lookup=lambda _refresh: self._owner(),
                run_lookup=self._run,
                apply=True,
                now=self.NOW,
            )


@pytest.mark.unit
def test_maintenance_wrapper_defaults_to_audit_and_wires_per_drop_fence(
    monkeypatch,
):
    import utils.maintenance_tasks as maintenance
    from scrapers.fbref.control import ControlStore

    run_id = "8ca16a99-4039-44a6-a47d-206037f11e70"
    events = []

    class FakeControl:
        def create_run(self, *_args, **_kwargs):
            return run_id

        def start_run(self, value):
            events.append(("start", value))

        def acquire_publication_lock(self, value, **_kwargs):
            events.append(("acquire", value))

        def renew_publication_lock(self, value, **kwargs):
            events.append(("renew", value, kwargs))

        def assert_publication_lock_owner(self, value, **kwargs):
            events.append(("assert", value, kwargs))

        def release_publication_lock(self, value):
            events.append(("release", value))

        def finish_run(self, value, *, succeeded):
            events.append(("finish", value, succeeded))

        def get_observation_cleanup_evidence(self, _refresh):
            return None

        def get_run(self, _run_id):
            return None

    fake = FakeControl()
    monkeypatch.delenv("FBREF_STAGE_JANITOR_MODE", raising=False)
    monkeypatch.setattr(ControlStore, "from_env", lambda: fake)

    captured = {}

    def fake_janitor(**kwargs):
        captured.update(kwargs)
        kwargs["before_drop"]("stage", "refresh")
        return {"attention_required_count": 0, "mode": "audit"}

    monkeypatch.setattr(
        maintenance, "janitor_fbref_generic_stages", fake_janitor
    )

    result = maintenance.maintain_fbref_generic_stages()

    assert captured["apply"] is False
    assert captured["run_lookup"] == fake.get_run
    assert [event[0] for event in events] == [
        "start",
        "acquire",
        "renew",
        "assert",
        "release",
        "finish",
    ]
    assert events[-1] == ("finish", run_id, True)
    assert result["control_run_id"] == run_id


@pytest.mark.unit
def test_maintenance_audit_mode_fails_when_drop_eligible_stage_remains(
    monkeypatch,
):
    import utils.maintenance_tasks as maintenance
    from scrapers.fbref.control import ControlStore

    finished = []

    class FakeControl:
        def create_run(self, *_args, **_kwargs):
            return "8ca16a99-4039-44a6-a47d-206037f11e70"

        def start_run(self, _run_id):
            pass

        def acquire_publication_lock(self, *_args, **_kwargs):
            pass

        def release_publication_lock(self, _run_id):
            pass

        def finish_run(self, _run_id, *, succeeded):
            finished.append(succeeded)

        def get_observation_cleanup_evidence(self, _refresh):
            return None

        def get_run(self, _run_id):
            return None

    monkeypatch.setattr(ControlStore, "from_env", FakeControl)
    monkeypatch.setattr(
        maintenance,
        "janitor_fbref_generic_stages",
        lambda **_kwargs: {
            "mode": "audit",
            "attention_required_count": 0,
            "eligible_count": 1,
        },
    )

    with pytest.raises(RuntimeError, match="audit_only_eligible=1"):
        maintenance.maintain_fbref_generic_stages(mode="audit")

    assert finished == [False]
