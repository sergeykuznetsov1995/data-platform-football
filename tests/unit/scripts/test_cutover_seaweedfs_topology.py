from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import subprocess
import textwrap


ROOT = Path(__file__).resolve().parents[3]


def test_cutover_is_quiesced_backup_gated_and_ordered() -> None:
    script = (ROOT / "scripts/cutover_seaweedfs_topology.sh").read_text()

    assert "SEAWEEDFS_CUTOVER_CONFIRM" in script
    assert "SEAWEEDFS_CUTOVER_IMAGE_ID" in script
    assert "rendered_volume" in script
    assert "live_volume" in script
    assert "supervised-v1" in script
    assert "topology_state_file" in script
    assert 'supervised_overlay="compose.seaweedfs-supervised.yaml"' in script
    assert 'docker compose -p "${SEAWEEDFS_COMPOSE_PROJECT_NAME}"' in script
    assert '-f compose.yaml -f "${supervised_overlay}"' in script
    assert "capacity-check" in script
    assert "whoscored_raw_backup.py preflight" in script
    assert "SEAWEEDFS_CUTOVER_REHEARSAL_INVENTORY" in script
    stop_marker = "Stopping every storage writer before the recovery cut"
    assert "readonly SEAWEEDFS_SUPERVISED_CUTOVER_AVAILABLE=false" in script
    assert "runtime-adoption and recovery audits" in script
    assert (
        'readonly SEAWEEDFS_S3_PROXY_CONFIG_SHA256="'
        "1f9cef7299e52272ee92ecaa4b58a413387291fd0d41626baf60da9768604a14\""
        in script
    )
    assert "assert_s3_proxy_config_boundary" in script
    assert "assert_host_protected_directory_chain" in script
    assert '[[ -L "${current}" ]]' in script
    assert '[[ "${current}" == / ]]' in script
    assert "(8#${path_mode} & 022)" in script
    assert "(8#${path_mode} & 01000)" in script
    assert '[[ -L "${config_path}" ]]' in script
    assert "(8#${config_mode} & 022)" in script
    assert 'config_owner}" != "0"' in script
    irreversible_gate = (
        'if [[ "${SEAWEEDFS_SUPERVISED_CUTOVER_AVAILABLE}" != true ]]'
    )
    proxy_config_preflight = script.index(
        "\nassert_s3_proxy_config_boundary\n", script.index("preflight_s3_config=")
    )
    assert proxy_config_preflight < script.index(irreversible_gate)
    assert script.index(irreversible_gate) < script.index(stop_marker)
    assert script.index("Verifying rehearsal backup access before downtime") < (
        script.index(stop_marker)
    )
    assert script.index("capacity-check") < script.index(stop_marker)
    assert "verify-backup" in script
    assert "verify-restore" in script
    assert script.index(
        'stop --timeout 120 seaweedfs-s3-proxy\n'
    ) < script.index(
        'stop --timeout 120 seaweedfs-s3\n'
    )
    assert script.index('stop --timeout 120 seaweedfs-s3\n') < script.index(
        'stop --timeout 120 seaweedfs\n'
    )
    assert script.index("stop --timeout 120 seaweedfs") < script.index(
        "up -d --no-deps seaweedfs-master"
    )
    assert script.index("legacy_running=") < script.index(
        "up -d --no-deps seaweedfs-master"
    )
    assert "Legacy weed mini is still running" in script
    assert "Legacy S3 gateway is still running" in script
    assert "S3 HTTP proxy is still running" in script
    assert 'rm -f seaweedfs-s3' in script
    permission_probe = 'test -w "$(dirname "$1")"'
    assert "install -d -o 50000 -g 0 -m 0770" in script
    assert script.index(permission_probe) < script.index(stop_marker)
    assert 'python3 - "${host_run_dir}/inventory.json"' not in script
    assert script.index("up -d --no-deps seaweedfs-master") < script.index(
        "up -d --no-deps seaweedfs-volume"
    )
    assert script.index("up -d --no-deps seaweedfs-volume") < script.index(
        "up -d --no-deps seaweedfs-filer"
    )
    proxy_start = script.index("up -d --no-deps seaweedfs-s3-proxy")
    assert script.rindex("assert_s3_proxy_config_boundary", 0, proxy_start) < proxy_start
    exit_trap = script.split("failed=1\n", 1)[1].split(
        "\nwait_healthy()", 1
    )[0]
    assert "authoritative docker ps confirms every writer and storage target" in (
        exit_trap
    )
    assert "{{.State.Running}}" not in exit_trap
    assert 'docker stop --time 120 "${service}"' in exit_trap
    assert "seaweedfs-s3-proxy seaweedfs-s3 seaweedfs" in exit_trap
    assert "seaweedfs-filer seaweedfs-volume seaweedfs-master" in exit_trap
    assert "docker ps --format '{{.Names}}'" in exit_trap
    assert "cutover quiescence is unproven" in exit_trap
    assert "status=125" in exit_trap
    assert exit_trap.index("trap - EXIT") < exit_trap.index('exit "${status}"')
    assert "label=com.docker.compose.oneoff=True" in script
    assert 'docker ps --all --filter "volume=${volume}"' in script
    assert 'docker ps --filter "volume=${volume}"' in script
    assert script.index('assert_no_running_volume_consumers "${live_volume}"') < (
        script.index("up -d --no-deps seaweedfs-master")
    )
    assert '"${writers[@]}"' in exit_trap
    assert 'for service in "${cutover_quiesce_targets[@]}"' in exit_trap
    secret_mount = script.split("/tmp/s3.config.json:ro", 1)[0].rsplit(
        '"${compose[@]}" run', 1
    )[1]
    assert "--user 0:0" in secret_mount


def _run_cutover_exit_trap(
    tmp_path: Path,
    *,
    running_inventory: str = "",
    ps_status: int = 0,
    exit_status: int = 37,
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    script = (ROOT / "scripts/cutover_seaweedfs_topology.sh").read_text(
        encoding="utf-8"
    )
    trap_source = script.split("failed=1\n", 1)[1].split(
        "\nwait_healthy()", 1
    )[0]
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls = tmp_path / "docker-calls"
    inventory = tmp_path / "running-containers"
    inventory.write_text(running_inventory, encoding="utf-8")
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -u
            printf '%s\n' "$*" >> "${FAKE_DOCKER_CALLS}"
            case "${1:-}" in
              stop)
                # Missing optional targets are normal; the engine-wide ps
                # inventory, not an individual stop result, is authoritative.
                exit 44
                ;;
              ps)
                if ((FAKE_DOCKER_PS_STATUS != 0)); then
                  exit "${FAKE_DOCKER_PS_STATUS}"
                fi
                cat "${FAKE_DOCKER_INVENTORY}"
                ;;
              *) exit 64 ;;
            esac
            """
        ),
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    topology_state_tmp = tmp_path / "prepared-state"
    topology_state_tmp.write_text("temporary", encoding="utf-8")
    fence_dir = tmp_path / "fence"
    fence_dir.mkdir()
    harness = textwrap.dedent(
        f"""\
        set -u
        writers=(writer-alpha writer-beta)
        topology_state_tmp={topology_state_tmp!s}
        fence_dir={fence_dir!s}
        failed=1
        {trap_source}
        exit {exit_status}
        """
    )
    result = subprocess.run(
        ["bash", "-c", harness],
        cwd=tmp_path,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "FAKE_DOCKER_CALLS": str(calls),
            "FAKE_DOCKER_INVENTORY": str(inventory),
            "FAKE_DOCKER_PS_STATUS": str(ps_status),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    return result, calls.read_text(encoding="utf-8").splitlines()


def test_cutover_exit_trap_stops_all_planes_and_preserves_safe_failure(
    tmp_path: Path,
) -> None:
    result, calls = _run_cutover_exit_trap(
        tmp_path,
        # Exact-name matching must not confuse this unrelated suffix with the
        # protected seaweedfs-s3-proxy target.
        running_inventory="seaweedfs-s3-proxy-shadow\nunrelated-service\n",
    )

    expected_targets = (
        "seaweedfs-s3-proxy",
        "seaweedfs-s3",
        "seaweedfs",
        "writer-alpha",
        "writer-beta",
        "seaweedfs-filer",
        "seaweedfs-volume",
        "seaweedfs-master",
    )
    assert result.returncode == 37
    assert calls == [
        *(f"stop --time 120 {target}" for target in expected_targets),
        "ps --format {{.Names}}",
    ]
    assert "authoritative docker ps confirms" in result.stderr
    assert "quiescence is unproven" not in result.stderr


def test_cutover_exit_trap_escalates_when_exact_target_remains_running(
    tmp_path: Path,
) -> None:
    result, calls = _run_cutover_exit_trap(
        tmp_path,
        running_inventory="unrelated-service\nseaweedfs-volume\n",
    )

    assert result.returncode == 125
    assert calls[-1] == "ps --format {{.Names}}"
    assert "target remains running after stop: seaweedfs-volume" in result.stderr
    assert "cutover quiescence is unproven" in result.stderr


def test_cutover_exit_trap_escalates_when_engine_inventory_fails(
    tmp_path: Path,
) -> None:
    result, calls = _run_cutover_exit_trap(tmp_path, ps_status=42)

    assert result.returncode == 125
    assert calls[-1] == "ps --format {{.Names}}"
    assert "docker ps enumeration failed" in result.stderr
    assert "cutover quiescence is unproven" in result.stderr


def test_routine_compose_and_make_preserve_supervised_topology_mode() -> None:
    wrapper = (ROOT / "scripts" / "compose.sh").read_text(encoding="utf-8")
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    legacy_guard = (
        ROOT / "scripts" / "seaweedfs_legacy_entrypoint.sh"
    ).read_text(encoding="utf-8")

    assert "seaweedfs-topology.mode" in wrapper
    assert "compose.seaweedfs-supervised.yaml" in wrapper
    assert "unset COMPOSE_FILE" in wrapper
    assert "override COMPOSE := ./scripts/compose.sh" in makefile
    assert "$(COMPOSE) down --remove-orphans" in makefile
    assert "$(COMPOSE) up -d --no-recreate" in makefile
    assert "down -v" not in makefile
    assert "force-recreate opensearch openmetadata-server" in makefile
    assert "elasticsearch" not in makefile
    assert "volume_name" in wrapper
    assert "inventory_sha256" in wrapper
    assert "volume_size_limit_mb" in wrapper
    assert "mini.options" in wrapper
    assert "docker image inspect" in wrapper
    assert ".supervised-topology-cutover-approved" in legacy_guard
    assert '"${1:-}" = "mini"' in legacy_guard
    assert "docker inspect seaweedfs-s3" in wrapper
    assert wrapper.count(
        '{{printf "%s\\t%s\\t%t" .Type .Source .RW}}'
    ) == 3
    assert '{{.Type}}\\t{{.Source}}\\t{{.RW}}' not in wrapper
    assert wrapper.count("| sed '/^$/d' | LC_ALL=C sort") == 3
    assert 'set(payload) != {"/data"}' in wrapper
    assert "/data:rw,noexec,nosuid,nodev,size=16m,mode=0700" in (
        ROOT / "compose.yaml"
    ).read_text(encoding="utf-8")


def _run_stateless_compose(
    tmp_path: Path,
    mode: str,
    *,
    rendered_volume: str = "seaweedfs_data",
    labeled_volumes: str = "",
    env_file: Path | None = None,
    state_file: Path | None = None,
    s3_config_payload: dict | None = None,
    shell_s3_credentials: tuple[str, str] | None = ("test-access", "test-secret"),
    rendered_s3_credentials: tuple[str, str] = ("test-access", "test-secret"),
    rendered_raw_credentials: tuple[str, str] = ("", ""),
    rendered_backup_credentials: tuple[str, str] = ("", ""),
    compose_args: tuple[str, ...] = ("up", "-d", "--no-recreate"),
) -> tuple[subprocess.CompletedProcess, str]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls = tmp_path / "docker-calls"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -u
            printf '%s\n' "$*" >> "${FAKE_DOCKER_CALLS}"
            case "${1:-}" in
              inspect)
                target="${!#}"
                if [[ ( "${FAKE_DOCKER_MODE}" == proxy-valid ||
                        "${FAKE_DOCKER_MODE}" == proxy-stop-drift ) &&
                      "${target}" == seaweedfs-s3-proxy ]]; then
                  case "$*" in
                    *'.Config.Cmd'*)
                      printf '%s\n' '["caddy","run","--config","/etc/caddy/S3ProxyCaddyfile","--adapter","caddyfile"]'
                      ;;
                    *'.Config.Entrypoint'*) printf '%s\n' null ;;
                    *'.Config.User'*) printf '%s\n' '65534:65534' ;;
                    *'.HostConfig.ReadonlyRootfs'*) printf '%s\n' true ;;
                    *'.HostConfig.CapDrop'*) printf '%s\n' '["ALL"]' ;;
                    *'.HostConfig.CapAdd'*)
                      printf '%s\n' '["CAP_NET_BIND_SERVICE"]'
                      ;;
                    *'.NetworkSettings.Networks'*)
                      printf '%s\n' dp-seaweedfs-control dp-storage
                      ;;
                    *'.HostConfig.PortBindings'*)
                      printf '%s\n' '{"8333/tcp":[{"HostIp":"127.0.0.1","HostPort":"8333"}]}'
                      ;;
                    *'/etc/caddy/S3ProxyCaddyfile'*)
                      printf 'bind\t%s/configs/seaweedfs/S3ProxyCaddyfile\tfalse\n' "${FAKE_REPO_ROOT}"
                      ;;
                    *'{{len .Mounts}}'*) printf '%s\n' 1 ;;
                    *'.HostConfig.Tmpfs'*)
                      printf '%s\n' '{"/tmp":"rw,noexec,nosuid,nodev,size=16m,mode=0700,uid=65534,gid=65534"}'
                      ;;
                    *'.HostConfig.SecurityOpt'*)
                      printf '%s\n' '["no-new-privileges:true"]'
                      ;;
                    *'.Config.StopSignal'*)
                      if [[ "${FAKE_DOCKER_MODE}" == proxy-stop-drift ]]; then
                        printf '%s\n' '"SIGKILL"'
                      else
                        printf '%s\n' '""'
                      fi
                      ;;
                    *'.Config.StopTimeout'*) printf '%s\n' '30' ;;
                    *'.HostConfig.RestartPolicy'*)
                      printf '%s\n' '{"Name":"unless-stopped","MaximumRetryCount":0}'
                      ;;
                    *'{{.Image}}'*)
                      printf '%s\n' 'sha256:4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d'
                      ;;
                    *'.Config.Image'*)
                      printf '%s\n' 'caddy:2.10-alpine@sha256:4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d'
                      ;;
                  esac
                  exit 0
                fi
                if [[ "${FAKE_DOCKER_MODE}" == supervised-container &&
                      "${target}" == seaweedfs-master ]]; then
                  exit 0
                fi
                case "${FAKE_DOCKER_MODE}:${target}" in
                  supervised-gateway:seaweedfs|legacy-container-valid:seaweedfs|legacy-container-stop-drift:seaweedfs|legacy-container-old-rollout:seaweedfs|legacy-container-null-rollout:seaweedfs|legacy-container-old-image-drift:seaweedfs|legacy-container-command-drift:seaweedfs|legacy-container-entrypoint-drift:seaweedfs|legacy-container-volume-drift:seaweedfs|legacy-container-image-drift:seaweedfs)
                    case "$*" in
                      *'.Config.Cmd'*)
                        if [[ "${FAKE_DOCKER_MODE}" == supervised-gateway ||
                              "${FAKE_DOCKER_MODE}" == legacy-container-command-drift ]]; then
                          printf '%s\n' '["s3","-port=8333"]'
                        elif [[ "${FAKE_DOCKER_MODE}" == legacy-container-old-rollout ||
                                "${FAKE_DOCKER_MODE}" == legacy-container-null-rollout ||
                                "${FAKE_DOCKER_MODE}" == legacy-container-old-image-drift ]]; then
                          printf '%s\n' '["mini","-dir=/data","-bucket=warehouse","-s3.config=/etc/seaweedfs/s3.config.json"]'
                        else
                          printf '%s\n' '["mini","-dir=/data","-bucket=warehouse","-s3.config=/etc/seaweedfs/s3.config.json","-s3=false","-s3.port.iceberg=0","-s3.iam=false","-webdav=false","-admin.ui=false","-filer.disableDirListing=true","-filer.ui.deleteDir=false"]'
                        fi
                        ;;
                      *'.Config.Entrypoint'*)
                        if [[ "${FAKE_DOCKER_MODE}" == legacy-container-entrypoint-drift ]]; then
                          printf '%s\n' '["/evil-entrypoint.sh"]'
                        elif [[ "${FAKE_DOCKER_MODE}" == legacy-container-old-rollout ||
                                "${FAKE_DOCKER_MODE}" == legacy-container-old-image-drift ]]; then
                          printf '%s\n' '["/entrypoint.sh"]'
                        elif [[ "${FAKE_DOCKER_MODE}" == legacy-container-null-rollout ]]; then
                          printf '%s\n' null
                        else
                          printf '%s\n' '["/usr/local/bin/seaweedfs-legacy-entrypoint"]'
                        fi
                        ;;
                      *'.HostConfig.PortBindings'*) printf '%s\n' '{}' ;;
                      *'.HostConfig.CapDrop'*) printf '%s\n' '["ALL"]' ;;
                      *'.HostConfig.CapAdd'*)
                        printf '%s\n' '["CAP_CHOWN","CAP_SETGID","CAP_SETUID","CAP_DAC_OVERRIDE","CAP_FOWNER"]'
                        ;;
                      *'.HostConfig.SecurityOpt'*)
                        printf '%s\n' '["no-new-privileges:true"]'
                        ;;
                      *'.HostConfig.ReadonlyRootfs'*) printf '%s\n' false ;;
                      *'.Config.User'*) printf '\n' ;;
                      *'.HostConfig.Tmpfs'*) printf '%s\n' '{}' ;;
                      *'.Config.StopSignal'*)
                        if [[ "${FAKE_DOCKER_MODE}" == legacy-container-stop-drift ]]; then
                          printf '%s\n' '"SIGKILL"'
                        else
                          printf '%s\n' '""'
                        fi
                        ;;
                      *'.Config.StopTimeout'*)
                        if [[ "${FAKE_DOCKER_MODE}" == legacy-container-old-rollout ||
                              "${FAKE_DOCKER_MODE}" == legacy-container-null-rollout ]]; then
                          printf '%s\n' '10'
                        else
                          printf '%s\n' '120'
                        fi
                        ;;
                      *'.HostConfig.RestartPolicy'*)
                        printf '%s\n' '{"Name":"unless-stopped","MaximumRetryCount":0}'
                        ;;
                      *'{{len .Mounts}}'*) printf '%s\n' 3 ;;
                      *'/etc/seaweedfs/s3.config.json'*)
                        printf 'bind\t%s\tfalse\n' "${FAKE_S3_CONFIG}"
                        ;;
                      *'/usr/local/bin/seaweedfs-legacy-entrypoint'*)
                        printf 'bind\t%s/scripts/seaweedfs_legacy_entrypoint.sh\tfalse\n' "${FAKE_REPO_ROOT}"
                        ;;
                      *'.Mounts'*)
                        if [[ "${FAKE_DOCKER_MODE}" == legacy-container-volume-drift ]]; then
                          printf '%s\n' old_seaweedfs_data
                        else
                          printf '%s\n' "${FAKE_RENDERED_VOLUME}"
                        fi
                        ;;
                      *'{{.Image}}'*)
                        if [[ "${FAKE_DOCKER_MODE}" == legacy-container-image-drift ||
                              "${FAKE_DOCKER_MODE}" == legacy-container-old-image-drift ]]; then
                          printf 'sha256:%064d\n' 0 | tr 0 b
                        else
                          printf '%s\n' 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                        fi
                        ;;
                      *'.Config.Image'*)
                        if [[ "${FAKE_DOCKER_MODE}" == legacy-container-old-rollout ||
                              "${FAKE_DOCKER_MODE}" == legacy-container-old-image-drift ||
                              "${FAKE_DOCKER_MODE}" == legacy-container-null-rollout ]]; then
                          printf '%s\n' 'chrislusf/seaweedfs:4.36'
                        else
                          printf '%s\n' 'chrislusf/seaweedfs:4.36@sha256:800b2115c63236e8bd0e5d572dc25dd493dc2feed08b54a2a269dc0101c9d94a'
                        fi
                        ;;
                      *'.NetworkSettings.Networks'*)
                        printf '%s\n' dp-seaweedfs-control
                        ;;
                    esac
                    exit 0
                    ;;
                esac
                if [[ "${target}" == seaweedfs-s3 ]]; then
                  case "$*" in
                    *'.NetworkSettings.Networks'*)
                      printf '%s\n' dp-seaweedfs-control dp-storage
                      ;;
                  esac
                fi
                if [[ "${FAKE_DOCKER_MODE}" == supervised-gateway &&
                      "${target}" == seaweedfs ]]; then
                  exit 0
                fi
                exit 1
                ;;
              volume)
                if [[ "${2:-}" == ls ]]; then
                  printf '%s' "${FAKE_LABELED_VOLUMES}"
                  exit 0
                fi
                [[ "${2:-}" == inspect ]] && exit 0
                ;;
              image)
                if [[ "${FAKE_DOCKER_MODE}" == missing-image &&
                      "$*" != *'caddy:2.10-alpine@sha256:'* ]]; then
                  exit 1
                fi
                if [[ "$*" == *'caddy:2.10-alpine@sha256:'* ]]; then
                  printf '%s\n' 'sha256:4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d'
                else
                  printf '%s\n' 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                fi
                exit 0
                ;;
              network)
                if [[ "${2:-}" == ls ]]; then
                  if [[ "${FAKE_DOCKER_MODE}" == network-drift ]]; then
                    printf '%s\n' dp-seaweedfs-control
                  fi
                  exit 0
                fi
                if [[ "${2:-}" == inspect &&
                      "${FAKE_DOCKER_MODE}" == network-drift ]]; then
                  printf '%s\n' '[{"Name":"dp-seaweedfs-control","Driver":"bridge","Scope":"local","Internal":false,"Attachable":false,"Labels":{"com.docker.compose.network":"seaweedfs-control"},"Containers":{}}]'
                  exit 0
                fi
                exit 1
                ;;
              ps)
                if [[ "${FAKE_DOCKER_MODE}" == running-oneoff ]]; then
                  printf '%s\t%s\n' aaaaaaaaaaaa airflow-scheduler
                elif [[ "${FAKE_DOCKER_MODE}" == unreviewed-volume-consumer &&
                        "$*" == *'--filter volume='* ]]; then
                  printf '%s\t%s\n' bbbbbbbbbbbb raw-volume-reader
                fi
                exit 0
                ;;
              run)
                if [[ "${FAKE_DOCKER_MODE}" == supervised-marker ||
                      "${FAKE_DOCKER_MODE}" == env-marker ]]; then
                  exit 0
                fi
                if [[ "${FAKE_DOCKER_MODE}" == legacy-unmarked ||
                      "${FAKE_DOCKER_MODE}" == frozen-volume ||
                      "${FAKE_DOCKER_MODE}" == legacy-container-valid ||
                      "${FAKE_DOCKER_MODE}" == legacy-container-stop-drift ||
                      "${FAKE_DOCKER_MODE}" == legacy-container-old-rollout ||
                      "${FAKE_DOCKER_MODE}" == legacy-container-null-rollout ||
                      "${FAKE_DOCKER_MODE}" == proxy-valid ||
                      "${FAKE_DOCKER_MODE}" == proxy-stop-drift ||
                      "${FAKE_DOCKER_MODE}" == network-drift ||
                      "${FAKE_DOCKER_MODE}" == unreviewed-volume-consumer ||
                      "${FAKE_DOCKER_MODE}" == orphaned-unmarked ]]; then
                  exit 3
                fi
                if [[ "${FAKE_DOCKER_MODE}" == orphaned-marker ]]; then
                  [[ "$*" == *"old_supervised_data:/data:ro"* ]] && exit 0
                  exit 3
                fi
                exit 64
                ;;
              compose)
                if [[ "$*" == *"config --format json"* ]]; then
                  printf '%s\n' "${FAKE_RENDERED_CONFIG}"
                elif [[ "${FAKE_DOCKER_MODE}" == frozen-volume ]]; then
                  [[ "${SEAWEEDFS_DATA_VOLUME_NAME:-}" == "${FAKE_RENDERED_VOLUME}" ]] || exit 65
                fi
                exit 0
                ;;
            esac
            exit 64
            """
        ),
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    lock = tmp_path / "topology.lock"
    lock.write_text("", encoding="utf-8")
    lock.chmod(0o600)
    s3_config = tmp_path / "s3.config.json"
    if s3_config_payload is None:
        s3_config_payload = {
            "identities": [
                {
                    "name": "test-admin",
                    "credentials": [
                        {"accessKey": "test-access", "secretKey": "test-secret"}
                    ],
                    "actions": ["Admin"],
                }
            ]
        }
    s3_config.write_text(json.dumps(s3_config_payload), encoding="utf-8")
    s3_config.chmod(0o600)
    airflow_environment = {
        "S3_ACCESS_KEY": rendered_s3_credentials[0],
        "S3_SECRET_KEY": rendered_s3_credentials[1],
        "ICEBERG_WAREHOUSE": "warehouse",
        "WHOSCORED_RAW_S3_ACCESS_KEY": rendered_raw_credentials[0],
        "WHOSCORED_RAW_S3_SECRET_KEY": rendered_raw_credentials[1],
    }
    scheduler_environment = {
        **airflow_environment,
        "WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY": rendered_backup_credentials[0],
        "WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY": rendered_backup_credentials[1],
    }
    config_mount = {
        "type": "bind",
        "source": str(s3_config),
        "target": "/etc/seaweedfs/s3.config.json",
        "read_only": True,
    }
    entrypoint_mount = {
        "type": "bind",
        "source": str(ROOT / "scripts/seaweedfs_legacy_entrypoint.sh"),
        "target": "/usr/local/bin/seaweedfs-legacy-entrypoint",
        "read_only": True,
    }
    rendered_config = {
        "name": "data-platform",
        "services": {
            "seaweedfs": {
                "command": [
                    "mini",
                    "-dir=/data",
                    "-bucket=warehouse",
                    "-s3.config=/etc/seaweedfs/s3.config.json",
                    "-s3=false",
                    "-s3.port.iceberg=0",
                    "-s3.iam=false",
                    "-webdav=false",
                    "-admin.ui=false",
                    "-filer.disableDirListing=true",
                    "-filer.ui.deleteDir=false",
                ],
                "entrypoint": ["/usr/local/bin/seaweedfs-legacy-entrypoint"],
                "volumes": [
                    {"type": "volume", "source": rendered_volume, "target": "/data"},
                    entrypoint_mount,
                    config_mount,
                ],
            },
            "seaweedfs-s3": {
                "command": [
                    "s3",
                    "-ip.bind=0.0.0.0",
                    "-port=8333",
                    "-filer=seaweedfs:8888",
                    "-config=/etc/seaweedfs/s3.config.json",
                    "-port.iceberg=0",
                    "-iam=false",
                    "-allowDeleteBucketNotEmpty=false",
                    "-concurrentFileUploadLimit=16",
                    "-concurrentUploadLimitMB=512",
                ],
                "entrypoint": ["/usr/local/bin/seaweedfs-legacy-entrypoint"],
                "tmpfs": ["/data:rw,noexec,nosuid,nodev,size=16m,mode=0700"],
                "volumes": [config_mount, entrypoint_mount],
            },
            "airflow-init": {"environment": airflow_environment},
            "airflow-scheduler": {"environment": scheduler_environment},
            "airflow-webserver": {"environment": airflow_environment},
            "trino": {
                "environment": {
                    "S3_ACCESS_KEY": rendered_s3_credentials[0],
                    "S3_SECRET_KEY": rendered_s3_credentials[1],
                }
            },
        },
        "volumes": {"seaweedfs_data": {"name": rendered_volume}},
    }
    command = ["bash", str(ROOT / "scripts" / "compose.sh")]
    if env_file is not None:
        command.extend(["--env-file", str(env_file)])
    command.extend(compose_args)
    run_environment = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DOCKER_CALLS": str(calls),
        "FAKE_DOCKER_MODE": mode,
        "FAKE_RENDERED_VOLUME": rendered_volume,
        "FAKE_LABELED_VOLUMES": labeled_volumes,
        "FAKE_RENDERED_CONFIG": json.dumps(
            rendered_config, separators=(",", ":")
        ),
        "FAKE_S3_CONFIG": str(s3_config),
        "FAKE_REPO_ROOT": str(ROOT),
        "FAKE_RENDERED_S3_ACCESS_KEY": rendered_s3_credentials[0],
        "FAKE_RENDERED_S3_SECRET_KEY": rendered_s3_credentials[1],
        "FAKE_RENDERED_RAW_ACCESS_KEY": rendered_raw_credentials[0],
        "FAKE_RENDERED_RAW_SECRET_KEY": rendered_raw_credentials[1],
        "FAKE_RENDERED_BACKUP_ACCESS_KEY": rendered_backup_credentials[0],
        "FAKE_RENDERED_BACKUP_SECRET_KEY": rendered_backup_credentials[1],
        "SEAWEEDFS_CUTOVER_LOCK_FILE": str(lock),
        "SEAWEEDFS_TOPOLOGY_STATE_FILE": str(
            state_file or tmp_path / "missing-state.json"
        ),
        "SEAWEEDFS_S3_CONFIG_FILE": str(s3_config),
        "ICEBERG_WAREHOUSE": "warehouse",
    }
    if shell_s3_credentials is not None:
        run_environment.update(
            {
                "S3_ACCESS_KEY": shell_s3_credentials[0],
                "S3_SECRET_KEY": shell_s3_credentials[1],
            }
        )
    else:
        run_environment.pop("S3_ACCESS_KEY", None)
        run_environment.pop("S3_SECRET_KEY", None)
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=run_environment,
        capture_output=True,
        text=True,
        check=False,
    )
    return result, calls.read_text(encoding="utf-8") if calls.exists() else ""


def test_stateless_upgrade_refuses_prior_supervised_containers(tmp_path) -> None:
    result, calls = _run_stateless_compose(tmp_path, "supervised-container")

    assert result.returncode == 78
    assert "adoption is required" in result.stderr
    assert not any(" up -d" in line for line in calls.splitlines())


def test_stateless_upgrade_refuses_prior_gateway_and_volume_marker(tmp_path) -> None:
    for mode in ("supervised-gateway", "supervised-marker"):
        case_dir = tmp_path / mode
        case_dir.mkdir()
        result, calls = _run_stateless_compose(case_dir, mode)

        assert result.returncode == 78
        assert "adoption is required" in result.stderr
        assert not any(" up -d" in line for line in calls.splitlines())


def test_routine_compose_rejects_storage_one_off_and_copy_bypasses(
    tmp_path,
) -> None:
    cases = (
        ("run", "--rm", "--entrypoint=/bin/sh", "seaweedfs", "mini"),
        ("run", "-T", "-v", "/tmp:/scratch", "--", "seaweedfs-master"),
        ("run", "-d", "airflow-scheduler", "true"),
        ("run", "--detach", "airflow-scheduler", "true"),
        ("exec", "-d", "airflow-scheduler", "true"),
        ("exec", "--detach", "airflow-scheduler", "true"),
        ("exec", "--no-tty", "seaweedfs-volume", "sh"),
        ("exec", "jupyterhub", "docker", "ps"),
        ("run", "--rm", "jupyterhub", "python", "-c", "print(1)"),
        ("attach", "--index", "1", "--sig-proxy", "seaweedfs"),
        ("commit", "--author", "operator", "--pause", "seaweedfs-master"),
        ("export", "-o", "snapshot.tar", "seaweedfs-filer"),
        ("cp", "local-file", "seaweedfs:/data/mini.options"),
        (
            "cp",
            "--dry-run",
            "--all",
            "--index",
            "1",
            "seaweedfs-filer:/data/file",
            "local-file",
        ),
        ("create", "seaweedfs"),
        ("start", "--dry-run", "seaweedfs-master"),
        ("start",),
        ("restart", "--no-deps", "--timeout", "10", "seaweedfs-filer"),
        ("restart",),
        ("scale", "--no-deps", "seaweedfs-master=2"),
        ("scale",),
        ("unpause", "seaweedfs-volume"),
        ("unpause",),
        ("pause", "seaweedfs-volume"),
        ("pause",),
        ("watch", "--no-up", "seaweedfs"),
        ("watch", "trino"),
        ("watch",),
        ("kill", "--signal", "SIGKILL", "seaweedfs-master"),
        ("kill",),
        ("stop", "seaweedfs"),
        ("stop",),
        ("rm", "-s", "-f", "seaweedfs"),
        ("wait", "--down-project", "airflow-init"),
        ("down", "--volumes"),
        ("rm", "-v", "seaweedfs"),
        ("rm", "-fv", "--dry-run", "airflow-scheduler"),
        ("rm", "-sv", "--dry-run", "airflow-scheduler"),
        ("rm", "-fsv", "--dry-run", "airflow-scheduler"),
        ("down", "-vt0", "--dry-run"),
        ("stop", "-t0", "seaweedfs"),
        ("stop", "--timeout", "0", "seaweedfs"),
        ("down", "-t0"),
        ("up", "-dt0", "--force-recreate", "seaweedfs"),
        ("up", "--timeout=0", "--force-recreate"),
        ("up", "--scale", "seaweedfs=0"),
        ("up", "--scale=seaweedfs-volume=0"),
        ("up", "--scale"),
        ("up", "--scale=not-a-scale"),
        ("down", "--rmi", "all"),
        ("down", "--rmi=local"),
    )
    for index, compose_args in enumerate(cases):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        result, calls = _run_stateless_compose(
            case_dir,
            "legacy-unmarked",
            compose_args=compose_args,
        )

        assert result.returncode == 78
        assert "forbidden" in result.stderr
        assert calls == ""


def test_routine_compose_rejects_whoscored_production_one_off(tmp_path) -> None:
    cases = (
        ("run", "--rm", "airflow-scheduler", "true"),
        ("exec", "airflow-scheduler", "true"),
        ("run", "--rm", "whoscored_proxy_filter", "true"),
        ("exec", "whoscored_proxy_filter", "true"),
    )
    for index, compose_args in enumerate(cases):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        result, calls = _run_stateless_compose(
            case_dir,
            "legacy-unmarked",
            compose_args=compose_args,
        )

        assert result.returncode == 78
        assert "production-gated WhoScored service" in result.stderr
        assert calls == ""


def test_routine_compose_rejects_protected_volume_on_non_storage_run(
    tmp_path,
) -> None:
    for index, volume_option in enumerate(
        (
            ("--volume", "seaweedfs_data:/scratch"),
            ("--volume=seaweedfs_data:/scratch",),
            ("-vseaweedfs_data:/scratch",),
            ("-v=seaweedfs_data:/scratch",),
        )
    ):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        result, calls = _run_stateless_compose(
            case_dir,
            "legacy-unmarked",
            compose_args=(
                "run",
                "--rm",
                *volume_option,
                "airflow-webserver",
                "true",
            ),
        )

        assert result.returncode == 78
        assert "may not mount the protected SeaweedFS volume" in result.stderr
        assert not any(" run --rm" in line for line in calls.splitlines())

    logical_dir = tmp_path / "logical-name"
    logical_dir.mkdir()
    result, calls = _run_stateless_compose(
        logical_dir,
        "legacy-unmarked",
        compose_args=(
            "run",
            "--rm",
            "-v=seaweedfs_data:/scratch",
            "airflow-webserver",
            "true",
        ),
    )
    assert result.returncode == 78
    assert "may not mount the protected SeaweedFS volume" in result.stderr
    assert not any(" run --rm" in line for line in calls.splitlines())


def test_routine_compose_config_cannot_write_files(tmp_path) -> None:
    cases = (
        ("config", "-o", str(tmp_path / "state")),
        ("config", f"-qo{tmp_path / 'state'}", "-q=false"),
        ("config", f"--output={tmp_path / 'state'}"),
        ("config", "--lock-image-digests"),
    )
    for index, compose_args in enumerate(cases):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        result, calls = _run_stateless_compose(
            case_dir,
            "legacy-unmarked",
            compose_args=compose_args,
        )

        assert result.returncode == 78
        assert "may not write output or image-lock files" in result.stderr
        assert calls == ""


def test_routine_compose_rejects_ambiguous_global_options(tmp_path) -> None:
    result, calls = _run_stateless_compose(
        tmp_path,
        "legacy-unmarked",
        compose_args=("--future-value-option", "ps", "up", "-d"),
    )

    assert result.returncode == 78
    assert "Unsupported Compose global option" in result.stderr
    assert calls == ""


def test_routine_compose_rejects_unreviewed_future_command_before_docker(
    tmp_path,
) -> None:
    result, calls = _run_stateless_compose(
        tmp_path,
        "legacy-unmarked",
        compose_args=("future-mutate", "seaweedfs"),
    )

    assert result.returncode == 78
    assert "refusing an unreviewed lifecycle verb" in result.stderr
    assert calls == ""


def test_routine_compose_rejects_project_name_override_before_docker(
    tmp_path,
) -> None:
    for index, project_args in enumerate(
        (("-p", "wrong-project"), ("--project-name=wrong-project",))
    ):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        result, calls = _run_stateless_compose(
            case_dir,
            "legacy-unmarked",
            compose_args=(*project_args, "ps"),
        )

        assert result.returncode == 78
        assert "Compose project must remain data-platform" in result.stderr
        assert calls == ""


def test_routine_compose_preserves_command_local_file_flags(tmp_path) -> None:
    cases = (
        ("logs", "-f", "airflow-scheduler"),
        ("rm", "-f", "airflow-scheduler"),
        (
            "run",
            "--rm",
            "airflow-webserver",
            "python",
            "-f",
            "--file",
            "payload.json",
        ),
    )
    for index, compose_args in enumerate(cases):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        result, calls = _run_stateless_compose(
            case_dir,
            "legacy-unmarked",
            compose_args=compose_args,
        )

        assert result.returncode == 0, result.stderr
        assert any(
            f" {compose_args[0]} " in f" {line} " for line in calls.splitlines()
        )


def _run_supervised_guard(
    tmp_path: Path,
    compose_args: tuple[str, ...],
) -> tuple[subprocess.CompletedProcess[str], str]:
    state = tmp_path / "topology.mode"
    state.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "mode": "supervised-v1",
                "volume_name": "protected_data",
                "image_id": "sha256:" + "a" * 64,
                "inventory_sha256": "b" * 64,
                "volume_size_limit_mb": 1024,
            }
        ),
        encoding="utf-8",
    )
    state.chmod(0o600)
    lock = tmp_path / "topology.lock"
    lock.write_text("", encoding="utf-8")
    lock.chmod(0o600)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    calls = tmp_path / "docker-calls"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >>\"$FAKE_DOCKER_CALLS\"\nexit 64\n",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    result = subprocess.run(
        ("bash", str(ROOT / "scripts" / "compose.sh"), *compose_args),
        cwd=ROOT,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "FAKE_DOCKER_CALLS": str(calls),
            "SEAWEEDFS_CUTOVER_LOCK_FILE": str(lock),
            "SEAWEEDFS_DATA_VOLUME_NAME": "protected_data",
            "SEAWEEDFS_TOPOLOGY_STATE_FILE": str(state),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    return result, calls.read_text(encoding="utf-8") if calls.exists() else ""


def test_supervised_state_rejects_unscoped_force_recreate_before_docker(
    tmp_path,
) -> None:
    result, calls = _run_supervised_guard(tmp_path, ("up", "--force-recreate"))

    assert result.returncode == 78
    assert "must use --no-recreate" in result.stderr
    assert calls == ""


def test_supervised_state_rejects_actual_and_logical_volume_run(tmp_path) -> None:
    for index, source in enumerate(("protected_data", "seaweedfs_data")):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        result, calls = _run_supervised_guard(
            case_dir,
            (
                "run",
                "--rm",
                f"-v={source}:/scratch",
                "airflow-webserver",
                "true",
            ),
        )

        assert result.returncode == 78
        assert "may not mount the protected SeaweedFS volume" in result.stderr
        assert calls == ""


def test_routine_compose_allows_parsed_non_storage_one_off(tmp_path) -> None:
    result, calls = _run_stateless_compose(
        tmp_path,
        "legacy-unmarked",
        compose_args=(
            "run",
            "--rm",
            "--no-deps",
            "--user",
            "1000:1000",
            "--volume",
            f"{ROOT}/configs/sofascore:/work/sofascore:ro",
            "airflow-webserver",
            "python",
            "--help",
        ),
    )

    assert result.returncode == 0, result.stderr
    assert any(" run --rm --no-deps" in line for line in calls.splitlines())


def test_routine_compose_rejects_host_root_and_docker_socket_binds(
    tmp_path,
) -> None:
    for index, volume_spec in enumerate(
        (
            "/:/host:ro",
            "/var/run/docker.sock:/var/run/docker.sock:rw",
            f"{ROOT}/configs/sofascore:/work/sofascore:ro:",
            f"{ROOT}/configs/sofascore:/work/sofascore:rw::",
        )
    ):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        result, calls = _run_stateless_compose(
            case_dir,
            "legacy-unmarked",
            compose_args=(
                "run",
                "--rm",
                "--no-deps",
                "--volume",
                volume_spec,
                "airflow-webserver",
                "true",
            ),
        )

        assert result.returncode == 78
        assert "outside the reviewed SofaScore registry bind" in result.stderr
        assert not any(" run --rm" in line for line in calls.splitlines())


def test_stateless_upgrade_rejects_env_file_volume_override(tmp_path) -> None:
    env_file = tmp_path / "production.env"
    env_file.write_text(
        "SEAWEEDFS_DATA_VOLUME_NAME=custom_supervised_data\n",
        encoding="utf-8",
    )
    result, calls = _run_stateless_compose(
        tmp_path,
        "env-marker",
        rendered_volume="custom_supervised_data",
        env_file=env_file,
    )

    assert result.returncode == 78
    assert "must retain volume seaweedfs_data" in result.stderr
    assert not any(" up -d" in line for line in calls.splitlines())


def test_stateless_upgrade_refuses_any_prior_differently_named_labeled_volume(
    tmp_path,
) -> None:
    for mode in ("orphaned-marker", "orphaned-unmarked"):
        case_dir = tmp_path / mode
        case_dir.mkdir()
        result, calls = _run_stateless_compose(
            case_dir,
            mode,
            rendered_volume="seaweedfs_data",
            labeled_volumes="old_seaweedfs_data\n",
        )

        assert result.returncode == 78
        assert "old_seaweedfs_data differs from the rendered" in result.stderr
        assert not any(" up -d" in line for line in calls.splitlines())


def test_stateless_upgrade_requires_exact_live_legacy_identity(tmp_path) -> None:
    for mode in (
        "legacy-container-command-drift",
        "legacy-container-entrypoint-drift",
        "legacy-container-volume-drift",
        "legacy-container-image-drift",
        "legacy-container-old-image-drift",
    ):
        case_dir = tmp_path / mode
        case_dir.mkdir()
        result, calls = _run_stateless_compose(case_dir, mode)

        assert result.returncode == 78
        assert "Existing SeaweedFS" in result.stderr
        assert not any(" up -d" in line for line in calls.splitlines())


def test_stateless_upgrade_allows_exact_live_legacy_identity(tmp_path) -> None:
    result, calls = _run_stateless_compose(tmp_path, "legacy-container-valid")

    assert result.returncode == 0, result.stderr
    assert any(" up -d" in line for line in calls.splitlines())


def test_stateless_down_rejects_unsafe_live_stop_signal(tmp_path) -> None:
    result, calls = _run_stateless_compose(
        tmp_path,
        "legacy-container-stop-drift",
        compose_args=("down", "--remove-orphans"),
    )

    assert result.returncode == 78
    assert "stop boundary differs" in result.stderr
    assert not any(line.endswith(" down --remove-orphans") for line in calls.splitlines())


def test_stateless_down_allows_known_old_identity_with_current_grace_period(
    tmp_path,
) -> None:
    result, calls = _run_stateless_compose(
        tmp_path,
        "legacy-container-old-rollout",
        compose_args=("down", "--remove-orphans"),
    )

    assert result.returncode == 0, result.stderr
    assert any(line.endswith(" down --remove-orphans") for line in calls.splitlines())


def test_stateless_upgrade_reconciles_known_old_legacy_entrypoints(tmp_path) -> None:
    for mode in ("legacy-container-old-rollout", "legacy-container-null-rollout"):
        case_dir = tmp_path / mode
        case_dir.mkdir()
        result, calls = _run_stateless_compose(case_dir, mode)

        assert result.returncode == 78
        assert "adoption is required" in result.stderr
        assert not any(line.startswith("compose ") and " up " in line for line in calls.splitlines())


def test_pre_isolation_legacy_requires_full_unscoped_up(tmp_path) -> None:
    result, calls = _run_stateless_compose(
        tmp_path,
        "legacy-container-old-rollout",
        compose_args=("up", "-d", "caddy"),
    )

    assert result.returncode == 78
    assert "adoption is required" in result.stderr
    assert not any(line.endswith(" up -d caddy") for line in calls.splitlines())


def test_running_writer_one_off_blocks_routine_mutation(tmp_path) -> None:
    result, calls = _run_stateless_compose(
        tmp_path,
        "running-oneoff",
        compose_args=("up", "-d"),
    )

    assert result.returncode == 78
    assert "Running one-off writer" in result.stderr
    assert "label=com.docker.compose.oneoff=True" in calls
    assert not any(line.startswith("compose ") for line in calls.splitlines())


def test_unreviewed_container_on_protected_volume_blocks_start(tmp_path) -> None:
    result, calls = _run_stateless_compose(
        tmp_path,
        "unreviewed-volume-consumer",
    )

    assert result.returncode == 78
    assert "Unreviewed container" in result.stderr
    assert "--all --filter volume=seaweedfs_data" in calls
    assert not any(line.endswith(" up -d --no-recreate") for line in calls.splitlines())


def test_existing_control_network_must_remain_internal(tmp_path) -> None:
    result, calls = _run_stateless_compose(tmp_path, "network-drift")

    assert result.returncode == 78
    assert "control network differs" in result.stderr
    assert "network inspect dp-seaweedfs-control" in calls
    assert not any(line.endswith(" up -d --no-recreate") for line in calls.splitlines())


def test_writer_resume_verbs_cannot_bypass_storage_boundary_preflight(
    tmp_path,
) -> None:
    cases = (
        ("start", "trino"),
        ("restart", "airflow-scheduler"),
        ("unpause", "trino"),
        ("scale", "airflow-scheduler=2"),
    )
    for index, compose_args in enumerate(cases):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        result, calls = _run_stateless_compose(
            case_dir,
            "network-drift",
            compose_args=compose_args,
        )

        assert result.returncode == 78
        assert "control network differs" in result.stderr
        assert not any(
            line.endswith(" " + " ".join(compose_args))
            for line in calls.splitlines()
        )


def test_proxy_runtime_accepts_docker_canonical_capability_name(tmp_path) -> None:
    result, calls = _run_stateless_compose(tmp_path, "proxy-valid")

    assert result.returncode == 0, result.stderr
    assert "CAP_NET_BIND_SERVICE" in (ROOT / "scripts/compose.sh").read_text()
    assert any(line.endswith(" up -d --no-recreate") for line in calls.splitlines())


def test_proxy_down_rejects_unsafe_live_stop_signal(tmp_path) -> None:
    result, calls = _run_stateless_compose(
        tmp_path,
        "proxy-stop-drift",
        compose_args=("down", "--remove-orphans"),
    )

    assert result.returncode == 78
    assert "proxy stop boundary differs" in result.stderr
    assert not any(line.endswith(" down --remove-orphans") for line in calls.splitlines())


def test_empty_s3_identities_cannot_start_storage_boundary(tmp_path) -> None:
    result, calls = _run_stateless_compose(
        tmp_path,
        "legacy-unmarked",
        s3_config_payload={"identities": []},
        compose_args=("up", "-d"),
    )

    assert result.returncode == 78
    assert "failed semantic validation" in result.stderr
    assert not any(line.endswith(" up -d") for line in calls.splitlines())


def test_s3_credentials_supplied_only_by_env_file_are_semantically_checked(
    tmp_path,
) -> None:
    env_file = tmp_path / "production.env"
    env_file.write_text(
        "S3_ACCESS_KEY=env-access\nS3_SECRET_KEY=env-secret\n",
        encoding="utf-8",
    )
    identity = {
        "identities": [
            {
                "name": "env-admin",
                "credentials": [
                    {"accessKey": "env-access", "secretKey": "env-secret"}
                ],
                "actions": ["Admin"],
            }
        ]
    }
    result, calls = _run_stateless_compose(
        tmp_path,
        "legacy-unmarked",
        env_file=env_file,
        s3_config_payload=identity,
        shell_s3_credentials=None,
        rendered_s3_credentials=("env-access", "env-secret"),
    )

    assert result.returncode == 0, result.stderr
    assert any(line.endswith(" up -d --no-recreate") for line in calls.splitlines())


def test_stateless_old_legacy_identity_cannot_start_without_recreate(
    tmp_path,
) -> None:
    cases = (
        ("restart", "airflow-scheduler"),
        ("up", "--no-recreate", "seaweedfs"),
    )
    for index, compose_args in enumerate(cases):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        result, calls = _run_stateless_compose(
            case_dir,
            "legacy-container-old-rollout",
            compose_args=compose_args,
        )

        assert result.returncode == 78
        assert not any(
            f" {compose_args[0]} " in line for line in calls.splitlines()
        )


def test_stateless_upgrade_allows_proven_unmarked_legacy_volume(tmp_path) -> None:
    result, calls = _run_stateless_compose(tmp_path, "legacy-unmarked")

    assert result.returncode == 0, result.stderr
    assert any(" up -d" in line for line in calls.splitlines())


def test_stateless_upgrade_freezes_audited_volume_for_final_compose(tmp_path) -> None:
    result, calls = _run_stateless_compose(
        tmp_path,
        "frozen-volume",
        rendered_volume="seaweedfs_data",
    )

    assert result.returncode == 0, result.stderr
    assert any(" up -d" in line for line in calls.splitlines())


def test_stateless_precreated_volume_requires_pinned_audit_image(tmp_path) -> None:
    result, calls = _run_stateless_compose(tmp_path, "missing-image")

    assert result.returncode == 78
    assert "docker pull chrislusf/seaweedfs:4.36@sha256:" in result.stderr
    assert not any(" up -d" in line for line in calls.splitlines())


def test_cutover_runbook_has_measured_rehearsal_and_empty_volume_recovery() -> None:
    runbook = (ROOT / "docs" / "operations" / "seaweedfs-topology-cutover.md").read_text(
        encoding="utf-8"
    )
    recovery_block = runbook.split("Any failure leaves writers stopped", 1)[1]

    assert "inventory-metrics.json" in runbook
    assert "backup-metrics.json" in runbook
    assert "verify-backup-metrics.json" in runbook
    assert "verify-source-metrics.json" in runbook
    assert "mib_per_second" in runbook
    assert "full-cutover-wall-seconds.txt" in runbook
    assert "SEAWEEDFS_DATA_VOLUME_NAME" in runbook
    assert "scripts/compose.sh" in runbook
    assert "compose.seaweedfs-supervised.yaml" in runbook
    assert 'docker volume inspect "$FAILED_SEAWEEDFS_DATA_VOLUME_NAME"' in runbook
    assert "docker volume create" in runbook
    assert 'docker volume inspect "$SEAWEEDFS_DATA_VOLUME_NAME"' in recovery_block
    assert "Refusing to reuse a pre-existing recovery volume" in recovery_block
    assert "find /data -mindepth 1 -maxdepth 1" in recovery_block
    assert "list-inventories" in runbook
    assert "fetch-inventory" in runbook
    assert "--create-bucket" in runbook
    assert "exclusive recovery access key is required" in runbook
    assert 'S3_ACCESS_KEY="$RECOVERY_S3_ACCESS_KEY"' in recovery_block
    assert "WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY" not in recovery_block
    assert runbook.index("fetch-inventory") < runbook.rindex("verify-restore")
    assert "transition_seaweedfs_recovery_state.py" in recovery_block
    assert "--lock-fd 9" in recovery_block
    assert '--expected-source-uri "s3://${ICEBERG_WAREHOUSE}"' in recovery_block
    assert recovery_block.index("acquire_seaweedfs_lifecycle_lock") < (
        recovery_block.index('stop --timeout 120')
    )
    assert 'readonly SEAWEEDFS_COMPOSE_PROJECT_NAME=data-platform' in recovery_block
    assert recovery_block.count('-p "$SEAWEEDFS_COMPOSE_PROJECT_NAME"') == 3
    assert (
        "SEAWEEDFS_S3_PROXY_CONFIG_SHA256="
        "1f9cef7299e52272ee92ecaa4b58a413387291fd0d41626baf60da9768604a14"
        in recovery_block
    )
    assert (
        "SEAWEEDFS_S3_PROXY_IMAGE='caddy:2.10-alpine@sha256:"
        "4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d'"
        in recovery_block
    )
    assert "assert_s3_proxy_config_boundary" in recovery_block
    assert "assert_host_protected_directory_chain" in recovery_block
    assert '[[ "$current" == / ]]' in recovery_block
    assert "assert_s3_proxy_runtime" in recovery_block
    assert "validate_s3_proxy_rendered_boundary" in recovery_block
    assert "audit_seaweedfs_runtime_container.py" in recovery_block
    assert "audit_seaweedfs_control_network.py" in recovery_block
    assert "Required SeaweedFS S3 proxy is missing" in recovery_block
    assert (
        'assert_s3_proxy_runtime current_compose "$FAILED_SEAWEEDFS_DATA_VOLUME_NAME"'
        in recovery_block
    )
    pre_stop_render = recovery_block.index(
        "validate_s3_proxy_rendered_boundary current_compose"
    )
    optional_live_proxy = recovery_block.index(
        "if docker inspect seaweedfs-s3-proxy >/dev/null 2>&1"
    )
    pre_stop_runtime = recovery_block.index(
        'assert_s3_proxy_runtime current_compose "$FAILED_SEAWEEDFS_DATA_VOLUME_NAME"'
    )
    assert pre_stop_render < optional_live_proxy < pre_stop_runtime
    assert recovery_block.count("up -d --no-deps seaweedfs-s3-proxy") == 2
    assert recovery_block.count("recovery_wait_healthy seaweedfs-s3-proxy") == 2
    assert (
        'assert_s3_proxy_runtime recovery_compose "$SEAWEEDFS_DATA_VOLUME_NAME"'
        in recovery_block
    )
    assert (
        'assert_s3_proxy_runtime final_compose "$SEAWEEDFS_DATA_VOLUME_NAME"'
        in recovery_block
    )
    final_proxy_start = recovery_block.rindex(
        '"${final_compose[@]}" up -d --no-deps seaweedfs-s3-proxy'
    )
    final_health = recovery_block.index(
        "'{{.State.Health.Status}}' seaweedfs-s3-proxy", final_proxy_start
    )
    assert final_proxy_start < final_health < recovery_block.index(
        "recovery_complete=1", final_health
    )
    fail_closed = recovery_block.split("rm -f", 1)[0]
    assert "seaweedfs-s3-proxy seaweedfs" in fail_closed
    assert 'for container in "${recovery_quiesce_targets[@]}"' in fail_closed
    assert 'docker stop --time 120 "$container"' in fail_closed
    assert "docker ps --format '{{.Names}}'" in fail_closed
    assert "Recovery target remains running" in fail_closed
    assert "EMERGENCY: recovery quiescence could not be confirmed" in fail_closed
    assert "status=125" in fail_closed
    assert (
        'stop --timeout 120 \\\n  seaweedfs-s3-proxy seaweedfs'
        in recovery_block
    )
    assert recovery_block.count("\nassert_seaweedfs_control_network\n") == 5
    first_network_audit = recovery_block.index(
        "\nassert_seaweedfs_control_network\n",
        recovery_block.index("current_compose=("),
    )
    first_stop = recovery_block.index('"${current_compose[@]}" stop --timeout 120')
    assert first_network_audit < first_stop

    recovery_start = recovery_block.index("recovery_compose=(")
    recovery_pre_network = recovery_block.index(
        "\nassert_seaweedfs_control_network\n", recovery_start
    )
    recovery_first_plane = recovery_block.index(
        '"${recovery_compose[@]}" up -d --no-deps "$service"',
        recovery_pre_network,
    )
    recovery_gateway = recovery_block.index(
        '"${recovery_compose[@]}" up -d --no-deps seaweedfs\n',
        recovery_first_plane,
    )
    recovery_proxy = recovery_block.index(
        '"${recovery_compose[@]}" up -d --no-deps seaweedfs-s3-proxy',
        recovery_gateway,
    )
    recovery_post_network = recovery_block.index(
        "\nassert_seaweedfs_control_network\n", recovery_proxy
    )
    assert recovery_pre_network < recovery_first_plane < recovery_gateway < (
        recovery_proxy
    ) < recovery_post_network

    final_start = recovery_block.index("final_compose=(")
    final_pre_network = recovery_block.index(
        "\nassert_seaweedfs_control_network\n", final_start
    )
    final_first_plane = recovery_block.index(
        '"${final_compose[@]}" up -d --no-deps "$service"', final_pre_network
    )
    final_gateway = recovery_block.index(
        '"${final_compose[@]}" up -d --no-deps seaweedfs\n', final_first_plane
    )
    final_post_network = recovery_block.index(
        "\nassert_seaweedfs_control_network\n", final_proxy_start
    )
    assert final_pre_network < final_first_plane < final_gateway < (
        final_proxy_start
    ) < final_post_network


def test_cutover_pins_and_attests_the_production_compose_project() -> None:
    source = (
        ROOT / "scripts" / "cutover_seaweedfs_topology.sh"
    ).read_text(encoding="utf-8")

    assert 'readonly SEAWEEDFS_COMPOSE_PROJECT_NAME="data-platform"' in source
    assert source.count('-p "${SEAWEEDFS_COMPOSE_PROJECT_NAME}"') == 4
    assert "com.docker.compose.project" in source
    assert "assert_compose_project_identity" in source


def test_cutover_rejects_group_readable_s3_credentials(tmp_path) -> None:
    script_path = ROOT / "scripts" / "cutover_seaweedfs_topology.sh"
    config = tmp_path / "configs" / "seaweedfs" / "s3.config.json"
    config.parent.mkdir(parents=True)
    config.write_text("{}", encoding="utf-8")
    config.chmod(0o640)
    lock = tmp_path / "cutover.lock"
    lock.write_text("", encoding="utf-8")
    lock.chmod(0o600)
    result = subprocess.run(
        ["bash", str(script_path)],
        cwd=tmp_path,
        env={
            **os.environ,
            "SEAWEEDFS_CUTOVER_LOCK_FILE": str(lock),
            "SEAWEEDFS_CUTOVER_CONFIRM": "backup-and-downtime-approved",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "must not grant group/other permissions" in result.stderr


def test_disabled_cutover_never_calls_docker_or_stops_writers(tmp_path) -> None:
    script_path = ROOT / "scripts" / "cutover_seaweedfs_topology.sh"
    config = tmp_path / "configs" / "seaweedfs" / "s3.config.json"
    config.parent.mkdir(parents=True)
    config.write_text("{}", encoding="utf-8")
    config.chmod(0o600)
    proxy_config = config.parent / "S3ProxyCaddyfile"
    proxy_config.write_bytes(
        (ROOT / "configs" / "seaweedfs" / "S3ProxyCaddyfile").read_bytes()
    )
    proxy_config.chmod(0o644)
    (tmp_path / "compose.seaweedfs-supervised.yaml").write_text(
        "services: {}\n", encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    state = tmp_path / "ps-count"
    state.write_text("0", encoding="utf-8")
    lock = tmp_path / "cutover.lock"
    lock.write_text("", encoding="utf-8")
    lock.chmod(0o600)
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -eu
            approved='sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
            if [[ "${1:-}" == "image" && "${2:-}" == "inspect" ]]; then
              echo "${approved}"
              exit 0
            fi
            if [[ "${1:-}" == "run" ]]; then
              echo 1024
              exit 0
            fi
            if [[ "${1:-}" == "inspect" ]]; then
              case "$*" in
                *Config.Cmd*) echo '["mini"]' ;;
                *Mounts*) echo seaweedfs_data ;;
                *"{{.Image}}"*) echo "${approved}" ;;
                *) echo false ;;
              esac
              exit 0
            fi
            [[ "${1:-}" == "compose" ]] || exit 64
            shift
            while [[ "${1:-}" == "-f" || "${1:-}" == "--env-file" ]]; do
              shift 2
            done
            case "${1:-}" in
              version) echo 2.24.4; exit 0 ;;
              config)
                if [[ "$*" == *"--format json"* ]]; then
                  echo '{"volumes":{"seaweedfs_data":{"name":"seaweedfs_data"}}}'
                fi
                exit 0
                ;;
              ps)
                count="$(cat "${FAKE_DOCKER_STATE}")"
                count=$((count + 1))
                printf '%s' "${count}" > "${FAKE_DOCKER_STATE}"
                if ((count == 1)); then
                  printf '%s\n' seaweedfs airflow-scheduler
                  exit 0
                fi
                exit 42
                ;;
              run) exit 0 ;;
              stop) exit 41 ;;
              *) exit 64 ;;
            esac
            """
        ),
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)

    result = subprocess.run(
        ["bash", str(script_path)],
        cwd=tmp_path,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "FAKE_DOCKER_STATE": str(state),
            "SEAWEEDFS_CUTOVER_LOCK_FILE": str(lock),
            "SEAWEEDFS_CUTOVER_CONFIRM": "backup-and-downtime-approved",
            "SEAWEEDFS_TOPOLOGY_STATE_FILE": str(tmp_path / "topology.mode"),
            "SEAWEEDFS_CUTOVER_IMAGE_ID": "sha256:" + "a" * 64,
            "SEAWEEDFS_DATA_VOLUME_NAME": "seaweedfs_data",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "code-owned disabled" in result.stderr
    assert state.read_text(encoding="utf-8") == "0"
    assert "writer shutdown could not be confirmed" not in result.stderr
    assert "all storage writers are stopped" not in result.stderr


def test_cutover_rejects_modified_s3_proxy_config_before_docker(tmp_path) -> None:
    script_path = ROOT / "scripts" / "cutover_seaweedfs_topology.sh"
    config_dir = tmp_path / "configs" / "seaweedfs"
    config_dir.mkdir(parents=True)
    s3_config = config_dir / "s3.config.json"
    s3_config.write_text("{}", encoding="utf-8")
    s3_config.chmod(0o600)
    proxy_config = config_dir / "S3ProxyCaddyfile"
    proxy_config.write_text(":8333 { respond 200 }\n", encoding="utf-8")
    proxy_config.chmod(0o644)
    lock = tmp_path / "cutover.lock"
    lock.write_text("", encoding="utf-8")
    lock.chmod(0o600)

    result = subprocess.run(
        ["bash", str(script_path)],
        cwd=tmp_path,
        env={
            **os.environ,
            "SEAWEEDFS_CUTOVER_LOCK_FILE": str(lock),
            "SEAWEEDFS_CUTOVER_CONFIRM": "backup-and-downtime-approved",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "S3 proxy config differs from the reviewed boundary" in result.stderr


def test_cutover_rejects_writable_s3_proxy_config_directory(tmp_path) -> None:
    script_path = ROOT / "scripts" / "cutover_seaweedfs_topology.sh"
    config_dir = tmp_path / "configs" / "seaweedfs"
    config_dir.mkdir(parents=True)
    s3_config = config_dir / "s3.config.json"
    s3_config.write_text("{}", encoding="utf-8")
    s3_config.chmod(0o600)
    proxy_config = config_dir / "S3ProxyCaddyfile"
    proxy_config.write_bytes(
        (ROOT / "configs" / "seaweedfs" / "S3ProxyCaddyfile").read_bytes()
    )
    proxy_config.chmod(0o644)
    config_dir.chmod(0o777)
    lock = tmp_path / "cutover.lock"
    lock.write_text("", encoding="utf-8")
    lock.chmod(0o600)

    result = subprocess.run(
        ["bash", str(script_path)],
        cwd=tmp_path,
        env={
            **os.environ,
            "SEAWEEDFS_CUTOVER_LOCK_FILE": str(lock),
            "SEAWEEDFS_CUTOVER_CONFIRM": "backup-and-downtime-approved",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "config directory chain is not host-protected" in result.stderr


def test_second_cutover_process_is_rejected_by_host_lock(tmp_path) -> None:
    script_path = ROOT / "scripts" / "cutover_seaweedfs_topology.sh"
    lock_path = tmp_path / "cutover.lock"
    lock_path.write_text("", encoding="utf-8")
    lock_path.chmod(0o600)
    with lock_path.open("r+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = subprocess.run(
            ["bash", str(script_path)],
            cwd=ROOT,
            env={
                **os.environ,
                "SEAWEEDFS_CUTOVER_LOCK_FILE": str(lock_path),
                "SEAWEEDFS_CUTOVER_CONFIRM": "backup-and-downtime-approved",
            },
            capture_output=True,
            text=True,
            check=False,
        )

    assert result.returncode == 73
    assert "already running" in result.stderr


def test_lifecycle_lock_rejects_group_or_world_read_access(tmp_path) -> None:
    script_path = ROOT / "scripts" / "compose.sh"
    lock_path = tmp_path / "topology.lock"
    lock_path.write_text("", encoding="utf-8")
    lock_path.chmod(0o640)

    result = subprocess.run(
        ["bash", str(script_path), "up"],
        cwd=ROOT,
        env={
            **os.environ,
            "SEAWEEDFS_CUTOVER_LOCK_FILE": str(lock_path),
            "SEAWEEDFS_TOPOLOGY_STATE_FILE": str(tmp_path / "missing-state.json"),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 73
    assert "lock file is not host-protected" in result.stderr


def test_routine_compose_rejects_unprotected_topology_state_directory(
    tmp_path,
) -> None:
    state_dir = tmp_path / "unprotected-state"
    state_dir.mkdir()
    state = state_dir / "topology.mode"
    state.write_text("{}", encoding="utf-8")
    state.chmod(0o644)
    state_dir.chmod(0o777)

    result, calls = _run_stateless_compose(
        tmp_path,
        "legacy-unmarked",
        state_file=state,
    )

    assert result.returncode == 78
    assert "state directory is not host-protected" in result.stderr
    assert calls == ""
