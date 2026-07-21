from __future__ import annotations

import stat
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WRAPPER = ROOT / "scripts/whoscored_daily_issuer.sh"
UNIT_ROOT = ROOT / "deploy/whoscored/systemd"


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
    assert "--max-scopes \"$MAX_SCOPES\"" in planner
    assert "approval-hmac" not in planner
    assert "owner-hmac" not in planner
    assert "issuance-ledger-hmac" not in planner

    assert "--network none" in signer
    assert "issue-daily-ingest" in signer
    assert "src=$authority_stage/cohort.json,dst=/authority/cohort.json,readonly" in signer
    assert "--cohort-file /authority/cohort.json" in signer
    assert '--max-scopes "$MAX_SCOPES"' in signer
    assert "--total-bytes \"$TOTAL_BYTES\"" in signer
    assert "--secret-file /run/credentials/approval-hmac" in signer
    assert "--owner-secret-file /run/credentials/owner-hmac" in signer
    assert (
        "--issuance-ledger-secret-file /run/credentials/issuance-ledger-hmac"
        in signer
    )
    assert "--force" not in text
    assert "readonly MAX_SCOPES=3" in text
    assert "readonly TOTAL_BYTES=135000000" in text


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
    assert 'src=$planner_output_dir,dst=/var/lib/whoscored/plans' in planner
    assert 'install -o root -g root -m 0440 "$planner_output_path"' in text
    assert 'require_frozen_container_file "$frozen_plan_host_path"' in text
    assert '(.scope_workloads | type == "array"' in text
    assert "length <= $max_scopes" in text
    assert ".schema_version == 2" in text
    assert ".max_scopes == $max_scopes" in text
    assert 'src=$frozen_plan_host_path,dst=$signer_plan_container_path,readonly' in signer
    assert "planner_output_dir" not in signer


def test_wrapper_binds_to_the_admitted_running_images() -> None:
    text = WRAPPER.read_text(encoding="utf-8")

    assert "require_digest_image \"$WHOSCORED_PLANNER_IMAGE\"" in text
    assert "require_digest_image \"$WHOSCORED_SIGNER_IMAGE\"" in text
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

    admission = text.index('"${admission_clean[@]}" verify-running')
    stage = text.index('authority_stage="$(mktemp')
    planner = text.index('"${docker_clean[@]}" run --rm --pull never')
    assert admission < stage < planner
    assert '--deployment-attestation "$WHOSCORED_DEPLOYMENT_ATTESTATION_FILE"' in text
    assert (
        '--deployment-admission-receipt '
        '"$WHOSCORED_DEPLOYMENT_ADMISSION_RECEIPT_FILE"' in text
    )
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


def test_canonical_path_guard_rejects_a_release_symlink(tmp_path: Path) -> None:
    text = WRAPPER.read_text(encoding="utf-8")
    function = "require_canonical_path() {" + text.split(
        "require_canonical_path() {", 1
    )[1].split("\n}\n", 1)[0] + "\n}"
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
    service = (UNIT_ROOT / "whoscored-daily-issuer.service").read_text(
        encoding="utf-8"
    )

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
