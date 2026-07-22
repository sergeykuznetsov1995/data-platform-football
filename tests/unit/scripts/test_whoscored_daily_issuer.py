from __future__ import annotations

import copy
import hashlib
import json
import stat
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WRAPPER = ROOT / "scripts/whoscored_daily_issuer.sh"
UNIT_ROOT = ROOT / "deploy/whoscored/systemd"


def _canonical_sha256(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def test_wrapper_is_executable_and_valid_bash() -> None:
    assert WRAPPER.stat().st_mode & stat.S_IXUSR
    subprocess.run(("bash", "-n", str(WRAPPER)), check=True)


def test_planner_and_signer_have_disjoint_authority() -> None:
    text = WRAPPER.read_text(encoding="utf-8")
    planner = text.split('"${docker_clean[@]}" run --rm --pull never', 1)[1].split(
        'require_container_private_file "$planner_output_path"', 1
    )[0]
    signer = text.split('"${docker_clean[@]}" run --rm --pull never', 2)[2]

    assert '--network "$PLANNER_NETWORK"' in planner
    assert '--env-file "$WHOSCORED_PLANNER_ENV_FILE"' in planner
    assert "plan-daily-ingest" in planner
    assert "--rollout-file /authority/rollout.json" in planner
    assert "--max-scopes" not in planner
    assert "approval-hmac" not in planner
    assert "owner-hmac" not in planner
    assert "issuance-ledger-hmac" not in planner

    assert "--network none" in signer
    assert "issue-daily-ingest" in signer
    assert (
        "src=$authority_stage/rollout.json,dst=/authority/rollout.json,readonly"
        in signer
    )
    assert "--rollout-file /authority/rollout.json" in signer
    assert "--max-scopes" not in signer
    assert "--total-bytes" not in signer
    assert "--secret-file /run/credentials/approval-hmac" in signer
    assert "--owner-secret-file /run/credentials/owner-hmac" in signer
    assert (
        "--issuance-ledger-secret-file /run/credentials/issuance-ledger-hmac" in signer
    )
    assert "--force" not in text
    assert "MAX_SCOPES=" not in text
    assert "TOTAL_BYTES=" not in text
    assert "WHOSCORED_ROLLOUT_FILE" in text
    assert "WHOSCORED_COHORT_FILE" not in text
    assert ".runtime_sha256 == $runtime" in text
    assert ".classifier_sha256 == $classifier" in text
    assert "promotion_acceptance_sha256" in text
    assert "promotion_terminal_receipt_sha256" in text
    assert "charter differs from the staged rollout/release" in text


def test_daily_plan_is_always_fresh_root_frozen_and_scope_bounded() -> None:
    text = WRAPPER.read_text(encoding="utf-8")
    planner = text.split('"${docker_clean[@]}" run --rm --pull never', 1)[1].split(
        'require_container_private_file "$planner_output_path"', 1
    )[0]
    signer = text.split('"${docker_clean[@]}" run --rm --pull never', 2)[2]

    assert "WHOSCORED_DAILY_PLAN_HOST_DIR" not in text
    assert 'planner_output_dir="$authority_stage/planner-output"' in text
    assert 'require_root_private_directory "$authority_stage"' in text
    assert "if test ! -e" not in planner
    assert "src=$planner_output_dir,dst=/var/lib/whoscored/plans" in planner
    assert 'install -o root -g root -m 0440 "$planner_output_path"' in text
    assert 'require_frozen_container_file "$frozen_plan_host_path"' in text
    assert '(.scope_workloads | type == "array"' in text
    assert "length == ([$max_scopes, $active_count] | min)" in text
    assert ".schema_version == 3" in text
    assert '.wave_id == "wave-20" and .max_scopes == 20' in text
    assert '.wave_id == "wave-70" and .max_scopes == 70' in text
    assert '.wave_id == "wave-all" and .max_scopes == 2000' in text
    assert ".ranked_scope_ids_sha256" in text
    assert (
        "src=$frozen_plan_host_path,dst=$signer_plan_container_path,readonly" in signer
    )
    assert "planner_output_dir" not in signer


def test_wrapper_binds_to_the_admitted_running_images() -> None:
    text = WRAPPER.read_text(encoding="utf-8")

    assert 'require_digest_image "$WHOSCORED_PLANNER_IMAGE"' in text
    assert 'require_digest_image "$WHOSCORED_SIGNER_IMAGE"' in text
    assert "inspect --format '{{.Config.Image}}' airflow-scheduler" in text
    assert "inspect --format '{{.Config.Image}}' whoscored_proxy_filter" in text
    assert "--pull never" in text
    assert 'readonly run_id="scheduled__${logical_date}"' in text
    assert 'readonly LOCK_PATH="$RUNTIME_DIRECTORY/issuer.lock"' in text
    assert '"$FLOCK" --exclusive --nonblock' in text


def test_wrapper_authority_directories_match_runtime_owner_contract() -> None:
    text = WRAPPER.read_text(encoding="utf-8")

    assert "50000:0:700|50000:0:750)" in text
    assert "50000:0:770" not in text


def test_wrapper_attests_running_services_before_planning() -> None:
    text = WRAPPER.read_text(encoding="utf-8")

    rollout_identity = text.index('issuance_rollout_id="$("$JQ" -er')
    admission = text.index('"${admission_clean[@]}" verify-running')
    live_authority = text.index(".issuance_rollout as $live")
    stage = text.index('authority_stage="$(mktemp')
    staged_copy = text.index(
        'install -o 50000 -g 0 -m 0400 "$source_path"', stage
    )
    staged_hash = text.index('staged_rollout_manifest_sha256="$(' , stage)
    staged_live_authority = text.index(
        '--slurpfile admission "$running_admission_receipt"', stage
    )
    planner = text.index('"${docker_clean[@]}" run --rm --pull never')
    assert (
        rollout_identity
        < admission
        < live_authority
        < stage
        < staged_copy
        < staged_hash
        < staged_live_authority
        < planner
    )
    assert '--deployment-attestation "$WHOSCORED_DEPLOYMENT_ATTESTATION_FILE"' in text
    assert (
        "--deployment-admission-receipt "
        '"$WHOSCORED_DEPLOYMENT_ADMISSION_RECEIPT_FILE"' in text
    )
    assert '--issuance-rollout-id "$issuance_rollout_id"' in text
    assert '$live.status == "live-authority-verified"' in text
    assert '$live.authority_binding == "current-signed-rollout"' in text
    assert "$live.promotion_acceptance_sha256 == $r.promotion_acceptance_sha256" in text
    assert (
        "$live.promotion_terminal_receipt_sha256 ==\n"
        "      $r.promotion_terminal_receipt_sha256" in text
    )
    assert "$live.authority == {" in text
    assert '"cohort_sha256": $c.cohort_sha256' in text
    assert '"runtime_sha256": $c.runtime_sha256' in text
    assert "$live.rollout_id == $c.rollout_id" in text
    assert "admitted-running-v1" in text
    for service in (
        "airflow-scheduler",
        "flaresolverr",
        "flaresolverr_whoscored_paid",
        "whoscored_paid_gateway",
        "whoscored_proxy_filter",
    ):
        assert f"--service {service}" in text
    assert "issuer may run only from 09:00 through 09:30 UTC" in text
    assert "staged rollout authority differs from fresh running admission" in text


def test_wrapper_accepts_only_exact_live_issuance_authority(tmp_path: Path) -> None:
    text = WRAPPER.read_text(encoding="utf-8")
    marker = '--arg rollout_id "$issuance_rollout_id" \'\n'
    jq_program = text.split(marker, 1)[1].split(
        '\n  \' "$running_admission_receipt"', 1
    )[0]
    rollout = {
        "schema_version": 3,
        "rollout_id": "production-rollout-2026-07",
        "wave_id": "wave-70",
        "max_scopes": 70,
        "require_full_active": False,
        "ranked_scope_ids_sha256": "a" * 64,
        "runtime_sha256": "b" * 64,
        "classifier_sha256": "c" * 64,
        "promotion_acceptance_sha256": "d" * 64,
        "promotion_terminal_receipt_sha256": "e" * 64,
    }
    charter = {
        **rollout,
        "schema_version": 4,
        "cohort_sha256": _canonical_sha256(rollout),
        "document_sha256": "f" * 64,
    }
    authority = {
        field: charter[field]
        for field in (
            "rollout_id",
            "wave_id",
            "max_scopes",
            "require_full_active",
            "cohort_sha256",
            "ranked_scope_ids_sha256",
            "runtime_sha256",
            "classifier_sha256",
            "promotion_acceptance_sha256",
            "promotion_terminal_receipt_sha256",
        )
    }
    response = {
        "schema_version": 2,
        "status": "admitted-running-v1",
        "images": [
            {"service": service, "final_image": image}
            for service, image in (
                ("airflow-scheduler", "planner-image"),
                ("flaresolverr", "other-image"),
                ("flaresolverr_whoscored_paid", "other-image"),
                ("whoscored_paid_gateway", "other-image"),
                ("whoscored_proxy_filter", "signer-image"),
            )
        ],
        "provider_policy": {"document_sha256": "1" * 64},
        "issuance_rollout": {
            "schema_version": 1,
            "status": "live-authority-verified",
            "authority_binding": "current-signed-rollout",
            "charter_sha256": charter["document_sha256"],
            "rollout_id": rollout["rollout_id"],
            "rollout_manifest_sha256": charter["cohort_sha256"],
            "wave_id": rollout["wave_id"],
            "promotion_acceptance_sha256": rollout["promotion_acceptance_sha256"],
            "promotion_terminal_receipt_sha256": rollout[
                "promotion_terminal_receipt_sha256"
            ],
            "authority": authority,
        },
    }
    rollout_path = tmp_path / "rollout.json"
    charter_path = tmp_path / "charter.json"
    response_path = tmp_path / "response.json"
    rollout_path.write_text(json.dumps(rollout), encoding="utf-8")
    charter_path.write_text(json.dumps(charter), encoding="utf-8")

    def validate(value) -> int:
        response_path.write_text(json.dumps(value), encoding="utf-8")
        return subprocess.run(
            (
                "/usr/bin/jq",
                "-e",
                "--slurpfile",
                "rollout",
                str(rollout_path),
                "--slurpfile",
                "charter",
                str(charter_path),
                "--arg",
                "planner",
                "planner-image",
                "--arg",
                "signer",
                "signer-image",
                "--arg",
                "rollout_id",
                rollout["rollout_id"],
                jq_program,
                str(response_path),
            ),
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode

    assert validate(response) == 0
    mutations = []
    for field, value in (
        ("charter_sha256", "4" * 64),
        ("promotion_acceptance_sha256", "0" * 64),
        ("promotion_terminal_receipt_sha256", "2" * 64),
        ("rollout_manifest_sha256", "5" * 64),
    ):
        mutation = copy.deepcopy(response)
        mutation["issuance_rollout"][field] = value
        mutations.append(mutation)
    authority_drift = copy.deepcopy(response)
    authority_drift["issuance_rollout"]["authority"]["runtime_sha256"] = "3" * 64
    mutations.append(authority_drift)
    extra_field = copy.deepcopy(response)
    extra_field["issuance_rollout"]["unreviewed"] = True
    mutations.append(extra_field)
    missing_live_replay = copy.deepcopy(response)
    del missing_live_replay["issuance_rollout"]
    mutations.append(missing_live_replay)

    assert all(validate(mutation) != 0 for mutation in mutations)


def test_wrapper_revalidates_staged_authority_after_copy(tmp_path: Path) -> None:
    text = WRAPPER.read_text(encoding="utf-8")
    post_stage = text.split(
        '--slurpfile admission "$running_admission_receipt"', 1
    )[1]
    marker = (
        '--arg staged_rollout_sha256 "$staged_rollout_manifest_sha256" \'\n'
    )
    jq_program = post_stage.split(marker, 1)[1].split(
        '\n  \' "$authority_stage/charter.json"', 1
    )[0]
    rollout = {
        "schema_version": 3,
        "rollout_id": "production-rollout-2026-07",
        "wave_id": "wave-70",
        "max_scopes": 70,
        "require_full_active": False,
        "ranked_scope_ids_sha256": "a" * 64,
        "runtime_sha256": "b" * 64,
        "classifier_sha256": "c" * 64,
        "promotion_acceptance_sha256": "d" * 64,
        "promotion_terminal_receipt_sha256": "e" * 64,
    }
    charter = {
        **rollout,
        "schema_version": 4,
        "cohort_sha256": _canonical_sha256(rollout),
        "document_sha256": "f" * 64,
    }
    authority = {
        field: charter[field]
        for field in (
            "rollout_id",
            "wave_id",
            "max_scopes",
            "require_full_active",
            "cohort_sha256",
            "ranked_scope_ids_sha256",
            "runtime_sha256",
            "classifier_sha256",
            "promotion_acceptance_sha256",
            "promotion_terminal_receipt_sha256",
        )
    }
    admission_response = {
        "issuance_rollout": {
            "schema_version": 1,
            "status": "live-authority-verified",
            "authority_binding": "current-signed-rollout",
            "charter_sha256": charter["document_sha256"],
            "rollout_id": rollout["rollout_id"],
            "rollout_manifest_sha256": charter["cohort_sha256"],
            "wave_id": rollout["wave_id"],
            "promotion_acceptance_sha256": rollout[
                "promotion_acceptance_sha256"
            ],
            "promotion_terminal_receipt_sha256": rollout[
                "promotion_terminal_receipt_sha256"
            ],
            "authority": authority,
        }
    }
    rollout_path = tmp_path / "staged-rollout.json"
    charter_path = tmp_path / "staged-charter.json"
    response_path = tmp_path / "running-admission.json"
    response_path.write_text(json.dumps(admission_response), encoding="utf-8")

    def validate(staged_rollout, staged_charter) -> int:
        rollout_path.write_text(json.dumps(staged_rollout), encoding="utf-8")
        charter_path.write_text(json.dumps(staged_charter), encoding="utf-8")
        return subprocess.run(
            (
                "/usr/bin/jq",
                "-e",
                "--slurpfile",
                "admission",
                str(response_path),
                "--slurpfile",
                "rollout",
                str(rollout_path),
                "--arg",
                "rollout_id",
                rollout["rollout_id"],
                "--arg",
                "staged_rollout_sha256",
                _canonical_sha256(staged_rollout),
                jq_program,
                str(charter_path),
            ),
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode

    assert validate(rollout, charter) == 0

    swapped_rollout = copy.deepcopy(rollout)
    swapped_charter = copy.deepcopy(charter)
    swapped_rollout["promotion_acceptance_sha256"] = "0" * 64
    swapped_charter["promotion_acceptance_sha256"] = "0" * 64
    swapped_charter["cohort_sha256"] = _canonical_sha256(swapped_rollout)
    assert validate(swapped_rollout, swapped_charter) != 0

    changed_cohort = copy.deepcopy(charter)
    changed_cohort["cohort_sha256"] = "1" * 64
    assert validate(rollout, changed_cohort) != 0

    changed_charter = copy.deepcopy(charter)
    changed_charter["document_sha256"] = "2" * 64
    assert validate(rollout, changed_charter) != 0


def test_canonical_path_guard_rejects_a_release_symlink(tmp_path: Path) -> None:
    text = WRAPPER.read_text(encoding="utf-8")
    function = (
        "require_canonical_path() {"
        + text.split("require_canonical_path() {", 1)[1].split("\n}\n", 1)[0]
        + "\n}"
    )
    target = tmp_path / "release-0123456789abcdef"
    target.mkdir()
    link = tmp_path / "current"
    link.symlink_to(target, target_is_directory=True)
    probe = f'fail() {{ exit 78; }}\n{function}\nrequire_canonical_path "$1"'

    canonical = subprocess.run(
        ("bash", "-c", probe, "canonical-path-probe", str(target)), check=False
    )
    symlinked = subprocess.run(
        ("bash", "-c", probe, "canonical-path-probe", str(link)), check=False
    )

    assert canonical.returncode == 0
    assert symlinked.returncode == 78


def test_timer_is_exact_utc_and_never_catches_up() -> None:
    timer = (UNIT_ROOT / "whoscored-daily-issuer.timer").read_text(encoding="utf-8")
    service = (UNIT_ROOT / "whoscored-daily-issuer.service").read_text(encoding="utf-8")

    assert "OnCalendar=*-*-* 09:15:00 UTC" in timer
    assert "Persistent=false" in timer
    assert "RandomizedDelaySec=0" in timer
    assert "EnvironmentFile=/etc/data-platform/whoscored-daily-issuer.env" in service
    assert service.count("LoadCredential=") == 3
    assert "PrivateNetwork=true" in service
    assert "RestrictAddressFamilies=AF_UNIX" in service
    assert "NoNewPrivileges=true" in service
    assert "CapabilityBoundingSet=CAP_CHOWN CAP_DAC_OVERRIDE" in service
    assert "ProtectSystem=strict" in service
    assert "RuntimeDirectory=whoscored-daily-issuer" in service
    assert "ReadWritePaths=/run/whoscored-daily-issuer" in service
    assert "TimeoutStartSec=30min" in service
