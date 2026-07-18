#!/usr/bin/env bash
# Select the persisted SeaweedFS topology for every routine Compose command.
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly SEAWEEDFS_IMAGE="chrislusf/seaweedfs:4.36@sha256:800b2115c63236e8bd0e5d572dc25dd493dc2feed08b54a2a269dc0101c9d94a"
readonly SEAWEEDFS_LEGACY_IMAGE="chrislusf/seaweedfs:4.36"
readonly SEAWEEDFS_LEGACY_VOLUME="seaweedfs_data"
readonly SEAWEEDFS_CONTROL_NETWORK="dp-seaweedfs-control"
readonly SEAWEEDFS_STORAGE_NETWORK="dp-storage"
readonly SEAWEEDFS_COMPOSE_PROJECT_NAME="data-platform"
readonly SEAWEEDFS_S3_PROXY_IMAGE="caddy:2.10-alpine@sha256:4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d"
readonly SEAWEEDFS_S3_PROXY_CONFIG_SHA256="1f9cef7299e52272ee92ecaa4b58a413387291fd0d41626baf60da9768604a14"
readonly SEAWEEDFS_S3_PROXY_COMMAND='["caddy","run","--config","/etc/caddy/S3ProxyCaddyfile","--adapter","caddyfile"]'
SEAWEEDFS_S3_PROXY_RESOLVED_ID=""
SEAWEEDFS_RENDERED_COMPOSE_MODEL=""
export COMPOSE_PROJECT_NAME="${SEAWEEDFS_COMPOSE_PROJECT_NAME}"
compose_command=""
compose_command_index=-1
up_has_no_recreate=0
up_has_force_recreate=0
up_has_no_deps=0
up_target_count=0
start_capable_command=0
runtime_boundary_command=0
compose_global_args=()
caller_args=("$@")
one_off_volume_sources=()
one_off_volume_specs=()
for ((index = 0; index < ${#caller_args[@]}; index++)); do
  argument="${caller_args[index]}"
  case "${argument}" in
    -f|--file|--project-directory)
      echo "Compose file/project overrides are forbidden; use the selected topology" >&2
      exit 78
      ;;
    -p|--project-name)
      if ((index + 1 >= ${#caller_args[@]})); then
        echo "Missing value for Compose global option ${argument}" >&2
        exit 78
      fi
      if [[ "${caller_args[index + 1]}" != "${SEAWEEDFS_COMPOSE_PROJECT_NAME}" ]]; then
        echo "Compose project must remain ${SEAWEEDFS_COMPOSE_PROJECT_NAME}" >&2
        exit 78
      fi
      compose_global_args+=("${argument}" "${caller_args[index + 1]}")
      ((index += 1))
      ;;
    --ansi|--env-file|--parallel|--profile|--progress)
      if ((index + 1 >= ${#caller_args[@]})); then
        echo "Missing value for Compose global option ${argument}" >&2
        exit 78
      fi
      compose_global_args+=("${argument}" "${caller_args[index + 1]}")
      ((index += 1))
      ;;
    -f?*|--file=*|--project-directory=*)
      echo "Compose file/project overrides are forbidden; use the selected topology" >&2
      exit 78
      ;;
    --project-name=*)
      if [[ "${argument#*=}" != "${SEAWEEDFS_COMPOSE_PROJECT_NAME}" ]]; then
        echo "Compose project must remain ${SEAWEEDFS_COMPOSE_PROJECT_NAME}" >&2
        exit 78
      fi
      compose_global_args+=("${argument}")
      ;;
    --ansi=*|--env-file=*|--parallel=*|--profile=*|--progress=*)
      compose_global_args+=("${argument}")
      ;;
    -p?*)
      project_name="${argument#-p}"
      project_name="${project_name#=}"
      if [[ "${project_name}" != "${SEAWEEDFS_COMPOSE_PROJECT_NAME}" ]]; then
        echo "Compose project must remain ${SEAWEEDFS_COMPOSE_PROJECT_NAME}" >&2
        exit 78
      fi
      compose_global_args+=("${argument}")
      ;;
    --all-resources|--compatibility|--dry-run|-h|--help|--version|--all-resources=*|--compatibility=*|--dry-run=*)
      compose_global_args+=("${argument}")
      ;;
    -*)
      echo "Unsupported Compose global option ${argument}; refusing ambiguous command parsing" >&2
      exit 78
      ;;
    *)
      compose_command="${argument}"
      compose_command_index="${index}"
      break
      ;;
  esac
done

case "${compose_command}" in
  attach|build|commit|config|cp|create|down|events|exec|export|images|kill|logs|ls|pause|port|ps|publish|pull|push|restart|rm|run|scale|start|stats|stop|top|unpause|up|version|volumes|wait|watch)
    ;;
  *)
    echo "Unsupported Compose command ${compose_command:-<empty>}; refusing an unreviewed lifecycle verb" >&2
    exit 78
    ;;
esac

compose_parse_error() {
  echo "Cannot safely parse Compose ${compose_command} target; ambiguous mutation is forbidden" >&2
  exit 78
}

reject_storage_service() {
  case "${1}" in
    seaweedfs|seaweedfs-*)
      echo "Routine Compose ${compose_command} is forbidden for SeaweedFS services; use the reviewed cutover/recovery lifecycle" >&2
      exit 78
      ;;
  esac
}

reject_docker_control_service() {
  if [[ "${1}" == jupyterhub ]]; then
    echo "Routine Compose ${compose_command} is forbidden for Docker-socket control service jupyterhub" >&2
    exit 78
  fi
}

reject_whoscored_production_one_off() {
  case "${1}" in
    airflow-scheduler|whoscored_proxy_filter|whoscored_paid_gateway|flaresolverr_whoscored_paid)
      echo "Routine Compose run/exec is forbidden for a production-gated WhoScored service" >&2
      exit 78
      ;;
  esac
}

record_one_off_volume_spec() {
  local volume_spec source
  volume_spec="${1}"
  source="${volume_spec%%:*}"
  if [[ "${volume_spec}" != *:* || -z "${source}" ]]; then
    compose_parse_error
  fi
  one_off_volume_sources+=("${source}")
  one_off_volume_specs+=("${volume_spec}")
}

validate_one_off_volume_specs() {
  local volume_spec source target mode remainder expected_source resolved_source
  expected_source="${repo_root}/configs/sofascore"
  for volume_spec in "${one_off_volume_specs[@]}"; do
    remainder=""
    IFS=: read -r source target mode remainder <<<"${volume_spec}"
    if [[ "${volume_spec}" != "${source}:${target}:${mode}" ]] ||
       [[ -n "${remainder}" ]] ||
       [[ "${source}" != "${expected_source}" ]] ||
       [[ "${target}" != /work/sofascore ]] ||
       [[ "${mode}" != ro && "${mode}" != rw ]] ||
       [[ ! -d "${source}" || -L "${source}" ]] ||
       ! resolved_source="$(realpath --canonicalize-existing -- "${source}")" ||
       [[ "${resolved_source}" != "${expected_source}" ]]; then
      echo "Routine Compose run volume is outside the reviewed SofaScore registry bind" >&2
      exit 78
    fi
  done
}

parse_one_off_service() {
  local verb token volume_spec service=""
  verb="${1}"
  shift
  while (($#)); do
    token="${1}"
    shift
    if [[ "${token}" == -- ]]; then
      (($#)) || compose_parse_error
      service="${1}"
      break
    fi
    case "${verb}:${token}" in
      run:--detach|run:-d|run:--detach=*|exec:--detach|exec:-d|exec:--detach=*)
        echo "Routine Compose detached run/exec is forbidden because it outlives the lifecycle lock" >&2
        exit 78
        ;;
      run:--volume|run:-v)
        (($#)) || compose_parse_error
        volume_spec="${1}"
        shift
        record_one_off_volume_spec "${volume_spec}"
        ;;
      run:--volume=*)
        volume_spec="${token#*=}"
        record_one_off_volume_spec "${volume_spec}"
        ;;
      run:-v?*)
        volume_spec="${token#-v}"
        volume_spec="${volume_spec#=}"
        record_one_off_volume_spec "${volume_spec}"
        ;;
      run:--cap-add|run:--cap-drop|run:--label|run:--publish|run:--pull|run:-l|run:-p)
        echo "Routine Compose run privilege, label, publish and pull overrides are forbidden" >&2
        exit 78
        ;;
      attach:--detach-keys|attach:--index|commit:--author|commit:-a|commit:--change|commit:-c|commit:--index|commit:--message|commit:-m|export:--index|export:--output|export:-o|run:--entrypoint|run:--env|run:--env-from-file|run:--name|run:--user|run:--workdir|run:-e|run:-u|run:-w|exec:--env|exec:--index|exec:--user|exec:--workdir|exec:-e|exec:-u|exec:-w)
        (($#)) || compose_parse_error
        shift
        ;;
      run:--cap-add=*|run:--cap-drop=*|run:--label=*|run:--publish=*|run:--pull=*|run:-l?*|run:-p?*)
        echo "Routine Compose run privilege, label, publish and pull overrides are forbidden" >&2
        exit 78
        ;;
      attach:--detach-keys=*|attach:--index=*|commit:--author=*|commit:-a?*|commit:--change=*|commit:-c?*|commit:--index=*|commit:--message=*|commit:-m?*|export:--index=*|export:--output=*|export:-o?*|run:--entrypoint=*|run:--env=*|run:--env-from-file=*|run:--name=*|run:--user=*|run:--workdir=*|run:-e?*|run:-u?*|run:-w?*|exec:--env=*|exec:--index=*|exec:--user=*|exec:--workdir=*|exec:-e?*|exec:-u?*|exec:-w?*)
        ;;
      run:--build|run:--remove-orphans|run:--service-ports|run:-P|run:--use-aliases|exec:--privileged)
        echo "Routine Compose one-off dependency, port and privilege overrides are forbidden" >&2
        exit 78
        ;;
      attach:--dry-run|attach:--no-stdin|attach:--sig-proxy|attach:--dry-run=*|attach:--no-stdin=*|attach:--sig-proxy=*|commit:--dry-run|commit:--pause|commit:-p|commit:--dry-run=*|commit:--pause=*|export:--dry-run|export:--dry-run=*|run:--dry-run|run:--interactive|run:-i|run:--no-deps|run:--no-TTY|run:-T|run:--quiet|run:-q|run:--quiet-build|run:--quiet-pull|run:--rm|exec:--dry-run|exec:--no-tty|exec:-T)
        ;;
      *:-*) compose_parse_error ;;
      *)
        service="${token}"
        break
        ;;
    esac
  done
  [[ -n "${service}" ]] || compose_parse_error
  reject_storage_service "${service}"
  reject_docker_control_service "${service}"
  reject_whoscored_production_one_off "${service}"
}

reject_protected_one_off_volume() {
  local protected_volume source
  protected_volume="${1}"
  for source in "${one_off_volume_sources[@]}"; do
    if [[ "${source}" == "${protected_volume}" ||
          "${source}" == seaweedfs_data ]]; then
      echo "Routine Compose run may not mount the protected SeaweedFS volume" >&2
      exit 78
    fi
  done
}

validate_s3_identity_config() {
  local encoded_values rendered_config validation_output validation_version
  local encoded_config encoded_admin_access encoded_admin_secret encoded_bucket
  local encoded_raw_access encoded_raw_secret encoded_backup_access encoded_backup_secret
  if ! rendered_config="$(
    docker compose "${compose_files[@]}" "${compose_global_args[@]}" \
      config --format json
  )"; then
    echo "Cannot render Compose before S3 identity validation" >&2
    exit 78
  fi
  SEAWEEDFS_RENDERED_COMPOSE_MODEL="${rendered_config}"
  if ! validation_output="$(
    python3 "${repo_root}/scripts/validate_seaweedfs_s3_identity_config.py" \
      <<<"${rendered_config}"
  )"; then
    echo "SeaweedFS S3 identity config failed semantic validation" >&2
    exit 78
  fi
  IFS='|' read -r encoded_config encoded_admin_access encoded_admin_secret \
    encoded_bucket encoded_raw_access encoded_raw_secret encoded_backup_access \
    encoded_backup_secret validation_version <<<"${validation_output}"
  if [[ "${validation_version}" != v1 ]] ||
     [[ "${validation_output}" != *'|'v1 ]]; then
    echo "Cannot parse validated S3 identity values" >&2
    exit 78
  fi
  encoded_values=(
    "${encoded_config}" "${encoded_admin_access}" "${encoded_admin_secret}"
    "${encoded_bucket}" "${encoded_raw_access}" "${encoded_raw_secret}"
    "${encoded_backup_access}" "${encoded_backup_secret}"
  )
  for encoded_value in "${encoded_values[@]}"; do
    if [[ ! "${encoded_value}" =~ ^[A-Za-z0-9+/]*={0,2}$ ]]; then
      echo "Cannot parse validated S3 identity values" >&2
      exit 78
    fi
  done
  SEAWEEDFS_S3_CONFIG_FILE="$(printf '%s' "${encoded_config}" | base64 --decode)"
  S3_ACCESS_KEY="$(printf '%s' "${encoded_admin_access}" | base64 --decode)"
  S3_SECRET_KEY="$(printf '%s' "${encoded_admin_secret}" | base64 --decode)"
  ICEBERG_WAREHOUSE="$(printf '%s' "${encoded_bucket}" | base64 --decode)"
  WHOSCORED_RAW_S3_ACCESS_KEY="$(printf '%s' "${encoded_raw_access}" | base64 --decode)"
  WHOSCORED_RAW_S3_SECRET_KEY="$(printf '%s' "${encoded_raw_secret}" | base64 --decode)"
  WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY="$(printf '%s' "${encoded_backup_access}" | base64 --decode)"
  WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY="$(printf '%s' "${encoded_backup_secret}" | base64 --decode)"
  export SEAWEEDFS_S3_CONFIG_FILE S3_ACCESS_KEY S3_SECRET_KEY ICEBERG_WAREHOUSE
  export WHOSCORED_RAW_S3_ACCESS_KEY WHOSCORED_RAW_S3_SECRET_KEY
  export WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY WHOSCORED_BACKUP_SOURCE_S3_SECRET_KEY
}

audit_supervised_runtime_container() {
  local container live_inspection
  container="${1}"
  if ! live_inspection="$(docker inspect "${container}")"; then
    echo "Cannot inspect protected SeaweedFS container ${container}" >&2
    exit 78
  fi
  if [[ -z "${SEAWEEDFS_RENDERED_COMPOSE_MODEL}" ]]; then
    echo "Rendered Compose model is unavailable for protected runtime audit" >&2
    exit 78
  fi
  if ! {
    printf '%s' "${SEAWEEDFS_RENDERED_COMPOSE_MODEL}" | base64 --wrap=0
    printf '\n'
    printf '%s' "${live_inspection}" | base64 --wrap=0
    printf '\n'
  } | python3 "${repo_root}/scripts/audit_seaweedfs_runtime_container.py" \
    "${container}" "${state_image_id}" "${state_volume}"; then
    echo "Protected SeaweedFS container ${container} differs from the rendered runtime boundary" >&2
    exit 78
  fi
}

validate_s3_proxy_config() {
  local config_path config_mode config_owner config_sha256
  local directory directory_mode directory_owner
  config_path="${repo_root}/configs/seaweedfs/S3ProxyCaddyfile"
  if [[ ! -f "${config_path}" ]] || [[ -L "${config_path}" ]]; then
    echo "SeaweedFS S3 proxy config must be a regular non-symlink file" >&2
    exit 78
  fi
  directory="$(dirname "${config_path}")"
  while :; do
    if [[ ! -d "${directory}" ]] || [[ -L "${directory}" ]]; then
      echo "SeaweedFS S3 proxy config path contains an unsafe directory" >&2
      exit 78
    fi
    directory_mode="$(stat -c '%a' "${directory}")"
    directory_owner="$(stat -c '%u' "${directory}")"
    if [[ "${directory_owner}" != "0" && "${directory_owner}" != "$(id -u)" ]] ||
       { (( (8#${directory_mode} & 022) != 0 )) &&
         { [[ "${directory_owner}" != "0" ]] ||
           (( (8#${directory_mode} & 01000) == 0 )); }; }; then
      echo "SeaweedFS S3 proxy config path is not host-protected" >&2
      exit 78
    fi
    [[ "${directory}" == / ]] && break
    directory="$(dirname "${directory}")"
  done
  config_mode="$(stat -c '%a' "${config_path}")"
  config_owner="$(stat -c '%u' "${config_path}")"
  if (( (8#${config_mode} & 022) != 0 )) ||
     [[ "${config_owner}" != "0" && "${config_owner}" != "$(id -u)" ]]; then
    echo "SeaweedFS S3 proxy config is not host-protected" >&2
    exit 78
  fi
  config_sha256="$(sha256sum "${config_path}")"
  config_sha256="${config_sha256%% *}"
  if [[ "${config_sha256}" != "${SEAWEEDFS_S3_PROXY_CONFIG_SHA256}" ]]; then
    echo "SeaweedFS S3 proxy config differs from the reviewed boundary" >&2
    exit 78
  fi
}

audit_s3_proxy_container() {
  local proxy_command proxy_entrypoint proxy_user proxy_read_only
  local proxy_cap_drop proxy_cap_add proxy_networks proxy_ports
  local proxy_mount proxy_mount_count proxy_tmpfs proxy_security
  local proxy_stop_signal proxy_stop_timeout proxy_restart
  if ! docker inspect seaweedfs-s3-proxy >/dev/null 2>&1; then
    return 0
  fi
  if ! proxy_command="$(docker inspect --format '{{json .Config.Cmd}}' seaweedfs-s3-proxy)" ||
     ! proxy_entrypoint="$(docker inspect --format '{{json .Config.Entrypoint}}' seaweedfs-s3-proxy)" ||
     ! proxy_user="$(docker inspect --format '{{.Config.User}}' seaweedfs-s3-proxy)" ||
     ! proxy_read_only="$(docker inspect --format '{{.HostConfig.ReadonlyRootfs}}' seaweedfs-s3-proxy)" ||
     ! proxy_cap_drop="$(docker inspect --format '{{json .HostConfig.CapDrop}}' seaweedfs-s3-proxy)" ||
     ! proxy_cap_add="$(docker inspect --format '{{json .HostConfig.CapAdd}}' seaweedfs-s3-proxy)" ||
     ! proxy_networks="$(docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' seaweedfs-s3-proxy | sed '/^$/d' | LC_ALL=C sort)" ||
     ! proxy_ports="$(docker inspect --format '{{json .HostConfig.PortBindings}}' seaweedfs-s3-proxy)" ||
     ! proxy_mount="$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/etc/caddy/S3ProxyCaddyfile"}}{{printf "%s\t%s\t%t" .Type .Source .RW}}{{end}}{{end}}' seaweedfs-s3-proxy)" ||
     ! proxy_mount_count="$(docker inspect --format '{{len .Mounts}}' seaweedfs-s3-proxy)" ||
     ! proxy_tmpfs="$(docker inspect --format '{{json .HostConfig.Tmpfs}}' seaweedfs-s3-proxy)" ||
     ! proxy_security="$(docker inspect --format '{{json .HostConfig.SecurityOpt}}' seaweedfs-s3-proxy)" ||
     ! proxy_stop_signal="$(docker inspect --format '{{json .Config.StopSignal}}' seaweedfs-s3-proxy)" ||
     ! proxy_stop_timeout="$(docker inspect --format '{{json .Config.StopTimeout}}' seaweedfs-s3-proxy)" ||
     ! proxy_restart="$(docker inspect --format '{{json .HostConfig.RestartPolicy}}' seaweedfs-s3-proxy)"; then
    echo "Cannot inspect the SeaweedFS S3 HTTP proxy boundary" >&2
    exit 78
  fi
  if [[ ! "${SEAWEEDFS_S3_PROXY_RESOLVED_ID}" =~ ^sha256:[0-9a-f]{64}$ ]] ||
     [[ "$(docker inspect --format '{{.Image}}' seaweedfs-s3-proxy)" != "${SEAWEEDFS_S3_PROXY_RESOLVED_ID}" ]] ||
     [[ "$(docker inspect --format '{{.Config.Image}}' seaweedfs-s3-proxy)" != "${SEAWEEDFS_S3_PROXY_IMAGE}" ]] ||
     [[ "${proxy_command}" != "${SEAWEEDFS_S3_PROXY_COMMAND}" ]] ||
     [[ "${proxy_entrypoint}" != null ]] ||
     [[ "${proxy_user}" != "65534:65534" ]] ||
     [[ "${proxy_read_only}" != true ]] ||
     [[ "${proxy_cap_drop}" != '["ALL"]' ]] ||
     [[ "${proxy_cap_add}" != '["CAP_NET_BIND_SERVICE"]' &&
        "${proxy_cap_add}" != '["NET_BIND_SERVICE"]' ]] ||
     [[ "${proxy_networks}" != "${SEAWEEDFS_CONTROL_NETWORK}"$'\n'"${SEAWEEDFS_STORAGE_NETWORK}" ]] ||
     [[ "${proxy_ports}" != '{"8333/tcp":[{"HostIp":"127.0.0.1","HostPort":"8333"}]}' ]] ||
     [[ "${proxy_mount}" != $'bind\t'"${repo_root}/configs/seaweedfs/S3ProxyCaddyfile"$'\tfalse' ]] ||
     [[ "${proxy_mount_count}" != 1 ]] ||
     [[ "${proxy_security}" != '["no-new-privileges"]' &&
        "${proxy_security}" != '["no-new-privileges:true"]' ]]; then
    echo "Existing SeaweedFS S3 HTTP proxy differs from its reviewed boundary; adoption is required" >&2
    exit 78
  fi
  if ! python3 - "${proxy_stop_signal}" "${proxy_stop_timeout}" \
    "${proxy_restart}" <<'PY'
import json
import sys

signal = json.loads(sys.argv[1]) or "SIGTERM"
timeout = json.loads(sys.argv[2])
restart = json.loads(sys.argv[3])
if signal.upper() != "SIGTERM" or timeout != 30:
    raise SystemExit(1)
if restart != {"Name": "unless-stopped", "MaximumRetryCount": 0}:
    raise SystemExit(1)
PY
  then
    echo "Existing SeaweedFS S3 HTTP proxy stop boundary differs from its reviewed identity" >&2
    exit 78
  fi
  if ! python3 - "${proxy_tmpfs}" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
if set(payload) != {"/tmp"}:
    raise SystemExit(1)
required = {
    "rw", "noexec", "nosuid", "nodev", "size=16m", "mode=0700",
    "uid=65534", "gid=65534",
}
if set(payload["/tmp"].split(",")) != required:
    raise SystemExit(1)
PY
  then
    echo "Existing SeaweedFS S3 HTTP proxy tmpfs differs from its reviewed boundary" >&2
    exit 78
  fi
}

audit_storage_stop_boundary() {
  local container expected_timeout stop_signal stop_timeout restart_policy
  container="${1}"
  expected_timeout="${2}"
  if ! stop_signal="$(docker inspect --format '{{json .Config.StopSignal}}' "${container}")" ||
     ! stop_timeout="$(docker inspect --format '{{json .Config.StopTimeout}}' "${container}")" ||
     ! restart_policy="$(docker inspect --format '{{json .HostConfig.RestartPolicy}}' "${container}")"; then
    echo "Cannot inspect the ${container} stop boundary" >&2
    exit 78
  fi
  if ! python3 - "${stop_signal}" "${stop_timeout}" \
    "${restart_policy}" "${expected_timeout}" <<'PY'
import json
import sys

signal = json.loads(sys.argv[1]) or "SIGTERM"
timeout = json.loads(sys.argv[2])
restart = json.loads(sys.argv[3])
expected_timeout = sys.argv[4]
if signal.upper() != "SIGTERM":
    raise SystemExit(1)
if expected_timeout and timeout != int(expected_timeout):
    raise SystemExit(1)
if restart != {"Name": "unless-stopped", "MaximumRetryCount": 0}:
    raise SystemExit(1)
PY
  then
    echo "Existing ${container} stop boundary differs from its reviewed identity" >&2
    exit 78
  fi
}

audit_seaweedfs_control_network() {
  local network_names network_model
  local -a allowed_members=(seaweedfs seaweedfs-s3-proxy)
  if ((state_active)); then
    allowed_members+=(seaweedfs-master seaweedfs-volume seaweedfs-filer)
  else
    allowed_members+=(seaweedfs-s3)
  fi
  if ! network_names="$(
    docker network ls \
      --filter "name=^${SEAWEEDFS_CONTROL_NETWORK}$" \
      --format '{{.Name}}'
  )"; then
    echo "Cannot enumerate the SeaweedFS control network" >&2
    exit 78
  fi
  if [[ -z "${network_names}" ]]; then
    # A fresh `up` will create the network from the already-rendered, reviewed
    # internal Compose model. Existing networks are never trusted implicitly.
    return 0
  fi
  if [[ "${network_names}" != "${SEAWEEDFS_CONTROL_NETWORK}" ]]; then
    echo "SeaweedFS control network lookup is ambiguous" >&2
    exit 78
  fi
  if ! network_model="$(docker network inspect "${SEAWEEDFS_CONTROL_NETWORK}")";
  then
    echo "Cannot inspect the SeaweedFS control network" >&2
    exit 78
  fi
  if ! python3 "${repo_root}/scripts/audit_seaweedfs_control_network.py" \
    "${SEAWEEDFS_CONTROL_NETWORK}" "${allowed_members[@]}" \
    <<<"${network_model}"; then
    echo "SeaweedFS control network differs from its reviewed internal boundary" >&2
    exit 78
  fi
}

audit_protected_volume_consumers() {
  local protected_volume consumers container_id container_name allowed=0
  local -a allowed_names=()
  protected_volume="${1}"
  shift
  allowed_names=("$@")
  if [[ ! "${protected_volume}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ ]]; then
    echo "Cannot audit an invalid protected SeaweedFS volume name" >&2
    exit 78
  fi
  if ! consumers="$(
    docker ps --all --filter "volume=${protected_volume}" \
      --format '{{.ID}}\t{{.Names}}'
  )"; then
    echo "Cannot enumerate containers using the protected SeaweedFS volume" >&2
    exit 78
  fi
  while IFS=$'\t' read -r container_id container_name; do
    [[ -n "${container_id}" ]] || continue
    if [[ ! "${container_id}" =~ ^[0-9a-f]{12,64}$ ]] ||
       [[ ! "${container_name}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]]; then
      echo "Cannot validate a protected SeaweedFS volume consumer" >&2
      exit 78
    fi
    allowed=0
    for expected_name in "${allowed_names[@]}"; do
      if [[ "${container_name}" == "${expected_name}" ]]; then
        allowed=1
        break
      fi
    done
    if ((!allowed)); then
      echo "Unreviewed container ${container_id} (${container_name}) uses protected SeaweedFS volume ${protected_volume}" >&2
      exit 78
    fi
  done <<<"${consumers}"
}

audit_legacy_storage_host_boundary() {
  local container kind expected_mount_count expected_config_mount expected_entrypoint_mount
  local port_bindings cap_drop cap_add security_options read_only user mount_count tmpfs
  container="${1}"
  kind="${2}"
  if [[ "${kind}" == mini ]]; then
    expected_mount_count=3
    audit_storage_stop_boundary "${container}" 120
  else
    expected_mount_count=2
    audit_storage_stop_boundary "${container}" 60
  fi
  if ! port_bindings="$(docker inspect --format '{{json .HostConfig.PortBindings}}' "${container}")" ||
     ! cap_drop="$(docker inspect --format '{{json .HostConfig.CapDrop}}' "${container}")" ||
     ! cap_add="$(docker inspect --format '{{json .HostConfig.CapAdd}}' "${container}")" ||
     ! security_options="$(docker inspect --format '{{json .HostConfig.SecurityOpt}}' "${container}")" ||
     ! read_only="$(docker inspect --format '{{.HostConfig.ReadonlyRootfs}}' "${container}")" ||
     ! user="$(docker inspect --format '{{.Config.User}}' "${container}")" ||
     ! mount_count="$(docker inspect --format '{{len .Mounts}}' "${container}")" ||
     ! tmpfs="$(docker inspect --format '{{json .HostConfig.Tmpfs}}' "${container}")" ||
     ! expected_config_mount="$(
       docker inspect --format '{{range .Mounts}}{{if eq .Destination "/etc/seaweedfs/s3.config.json"}}{{printf "%s\t%s\t%t" .Type .Source .RW}}{{end}}{{end}}' "${container}"
     )" ||
     ! expected_entrypoint_mount="$(
       docker inspect --format '{{range .Mounts}}{{if eq .Destination "/usr/local/bin/seaweedfs-legacy-entrypoint"}}{{printf "%s\t%s\t%t" .Type .Source .RW}}{{end}}{{end}}' "${container}"
     )"; then
    echo "Cannot inspect the ${kind} SeaweedFS host boundary" >&2
    exit 78
  fi
  if [[ "${port_bindings}" != '{}' && "${port_bindings}" != null ]] ||
     [[ "${cap_drop}" != '["ALL"]' ]] ||
     [[ "${security_options}" != '["no-new-privileges"]' &&
        "${security_options}" != '["no-new-privileges:true"]' ]] ||
     [[ "${read_only}" != false ]] || [[ -n "${user}" ]] ||
     [[ "${mount_count}" != "${expected_mount_count}" ]] ||
     [[ "${expected_config_mount}" != $'bind\t'"${SEAWEEDFS_S3_CONFIG_FILE}"$'\tfalse' ]] ||
     [[ "${expected_entrypoint_mount}" != $'bind\t'"${repo_root}/scripts/seaweedfs_legacy_entrypoint.sh"$'\tfalse' ]]; then
    echo "Existing ${kind} SeaweedFS host boundary differs from its reviewed identity" >&2
    exit 78
  fi
  if ! python3 - "${kind}" "${cap_add}" <<'PY'
import json
import sys

kind = sys.argv[1]
payload = json.loads(sys.argv[2])
actual = set(payload or [])
expected = set()
if kind == "mini":
    expected = {
        "CAP_CHOWN", "CAP_SETGID", "CAP_SETUID", "CAP_DAC_OVERRIDE", "CAP_FOWNER"
    }
normalised = {item if item.startswith("CAP_") else f"CAP_{item}" for item in actual}
if normalised != expected:
    raise SystemExit(1)
PY
  then
    echo "Existing ${kind} SeaweedFS capabilities differ from its reviewed identity" >&2
    exit 78
  fi
  if ! python3 - "${kind}" "${tmpfs}" <<'PY'
import json
import sys

kind = sys.argv[1]
payload = json.loads(sys.argv[2]) or {}
if kind == "mini":
    if payload:
        raise SystemExit(1)
else:
    if set(payload) != {"/data"}:
        raise SystemExit(1)
    required = {
        "rw", "noexec", "nosuid", "nodev", "size=16m", "mode=0700"
    }
    if set(payload["/data"].split(",")) != required:
        raise SystemExit(1)
PY
  then
    echo "Existing ${kind} SeaweedFS tmpfs boundary differs from its reviewed identity" >&2
    exit 78
  fi
}

audit_runtime_preconditions() {
  local running_one_offs one_off_id one_off_service proxy_image_id
  if ! running_one_offs="$(
    docker ps \
      --filter label=com.docker.compose.oneoff=True \
      --format '{{.ID}}\t{{.Label "com.docker.compose.service"}}'
  )"; then
    echo "Cannot enumerate running Compose one-off containers" >&2
    exit 78
  fi
  while IFS=$'\t' read -r one_off_id one_off_service; do
    [[ -n "${one_off_id}" ]] || continue
    if [[ ! "${one_off_id}" =~ ^[0-9a-f]{12,64}$ ]] ||
       [[ ! "${one_off_service}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]]; then
      echo "Cannot validate a running Compose one-off container" >&2
      exit 78
    fi
    case "${one_off_service}" in
      airflow-init|airflow-scheduler|airflow-webserver|lakekeeper-migrate|lakekeeper|trino|superset|superset-worker|superset-beat|openmetadata-migrate|openmetadata-server|openmetadata-ingestion|jupyterhub|seaweedfs|seaweedfs-*)
        echo "Running one-off writer ${one_off_id} (${one_off_service}) blocks storage lifecycle mutation" >&2
        exit 78
        ;;
    esac
  done <<<"${running_one_offs}"
  if ((runtime_boundary_command)); then
    validate_s3_identity_config
    validate_s3_proxy_config
    if ! proxy_image_id="$(
      docker image inspect --format '{{.Id}}' "${SEAWEEDFS_S3_PROXY_IMAGE}" 2>/dev/null
    )" || [[ ! "${proxy_image_id}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
      echo "Pinned SeaweedFS S3 proxy image is absent; run: docker pull ${SEAWEEDFS_S3_PROXY_IMAGE}" >&2
      exit 78
    fi
    SEAWEEDFS_S3_PROXY_RESOLVED_ID="${proxy_image_id}"
    audit_seaweedfs_control_network
    audit_s3_proxy_container
  fi
}

parse_cp_endpoints() {
  local token endpoint service
  local -a endpoints=()
  while (($#)); do
    token="${1}"
    shift
    if [[ "${token}" == -- ]]; then
      while (($#)); do
        endpoints+=("${1}")
        shift
      done
      break
    fi
    case "${token}" in
      --all|--archive|-a|--dry-run|--follow-link|-L) ;;
      --index)
        (($#)) || compose_parse_error
        shift
        ;;
      --index=*) ;;
      -) endpoints+=("${token}") ;;
      -*) compose_parse_error ;;
      *) endpoints+=("${token}") ;;
    esac
  done
  ((${#endpoints[@]} == 2)) || compose_parse_error
  for endpoint in "${endpoints[@]}"; do
    [[ "${endpoint}" == *:* ]] || continue
    service="${endpoint%%:*}"
    reject_storage_service "${service}"
    reject_docker_control_service "${service}"
  done
}

parse_mutating_service_targets() {
  local verb token service target_count=0
  verb="${1}"
  shift
  while (($#)); do
    token="${1}"
    shift
    if [[ "${token}" == -- ]]; then
      (($#)) || compose_parse_error
      while (($#)); do
        service="${1}"
        [[ "${verb}" != scale ]] || service="${service%%=*}"
        reject_storage_service "${service}"
        ((target_count += 1))
        shift
      done
      break
    fi
    case "${verb}:${token}" in
      kill:--dry-run|kill:--remove-orphans|pause:--dry-run|rm:--dry-run|rm:--force|rm:-f|rm:--stop|rm:-s|start:--dry-run|stop:--dry-run|restart:--dry-run|restart:--no-deps|scale:--dry-run|scale:--no-deps|unpause:--dry-run|watch:--dry-run|watch:--no-up|watch:--prune|watch:--quiet)
        ;;
      kill:--dry-run=*|kill:--remove-orphans=*|pause:--dry-run=*|rm:--dry-run=*|rm:--force=*|rm:--stop=*|start:--dry-run=*|stop:--dry-run=*|restart:--dry-run=*|restart:--no-deps=*|scale:--dry-run=*|scale:--no-deps=*|unpause:--dry-run=*|watch:--dry-run=*|watch:--no-up=*|watch:--prune=*|watch:--quiet=*)
        ;;
      kill:--signal|kill:-s|restart:--timeout|restart:-t)
        (($#)) || compose_parse_error
        shift
        ;;
      kill:--signal=*|kill:-s?*|restart:--timeout=*|restart:-t?*) ;;
      *:-*) compose_parse_error ;;
      *)
        service="${token}"
        [[ "${verb}" != scale ]] || service="${service%%=*}"
        reject_storage_service "${service}"
        ((target_count += 1))
        ;;
    esac
  done
  if ((target_count == 0)); then
    echo "Routine Compose ${verb} without explicit non-storage services is forbidden" >&2
    exit 78
  fi
}

if ((compose_command_index >= 0)); then
  command_tail=("${caller_args[@]:compose_command_index + 1}")
  if [[ "${compose_command}" == up ]]; then
    for ((index = 0; index < ${#command_tail[@]}; index++)); do
      argument="${command_tail[index]}"
      scale_spec=""
      case "${argument}" in
        --)
          for ((index += 1; index < ${#command_tail[@]}; index++)); do
            service="${command_tail[index]}"
            [[ "${service}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || compose_parse_error
            reject_storage_service "${service}"
            ((up_target_count += 1))
          done
          break
          ;;
        --scale)
          if ((index + 1 >= ${#command_tail[@]})); then
            compose_parse_error
          fi
          scale_spec="${command_tail[index + 1]}"
          ((index += 1))
          ;;
        --scale=*) scale_spec="${argument#*=}" ;;
        --attach|--exit-code-from|--no-attach|--pull|--wait-timeout)
          if ((index + 1 >= ${#command_tail[@]})); then
            compose_parse_error
          fi
          ((index += 1))
          ;;
        --attach=*|--exit-code-from=*|--no-attach=*|--pull=*|--wait-timeout=*) ;;
        --no-recreate) up_has_no_recreate=1 ;;
        --force-recreate) up_has_force_recreate=1 ;;
        --no-deps) up_has_no_deps=1 ;;
        --always-recreate-deps)
          echo "Routine Compose up may not recreate storage dependencies" >&2
          exit 78
          ;;
        --abort-on-container-exit|--abort-on-container-failure|--attach-dependencies|--build|--detach|--dry-run|--menu|--no-build|--no-color|--no-log-prefix|--no-start|--quiet-build|--quiet-pull|--remove-orphans|--renew-anon-volumes|--timestamps|--wait|--watch|--yes|--menu=*|--dry-run=*) ;;
        -[dVwy]*)
          [[ "${argument}" =~ ^-[dVwy]+$ ]] || compose_parse_error
          ;;
        -*) compose_parse_error ;;
        *)
          [[ "${argument}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || compose_parse_error
          reject_storage_service "${argument}"
          ((up_target_count += 1))
          ;;
      esac
      if [[ -n "${scale_spec}" ]]; then
        if [[ ! "${scale_spec}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*=[0-9]+$ ]]; then
          compose_parse_error
        fi
        reject_storage_service "${scale_spec%%=*}"
      elif [[ "${argument}" == --scale=* ]]; then
        compose_parse_error
      fi
    done
  fi
  case "${compose_command}" in
    down|stop|up)
      for argument in "${command_tail[@]}"; do
        if [[ "${argument}" =~ ^-[^-]*t ]]; then
          echo "Routine Compose ${compose_command} timeout overrides are forbidden for SeaweedFS durability" >&2
          exit 78
        fi
        case "${argument}" in
          --timeout|--timeout=*)
            echo "Routine Compose ${compose_command} timeout overrides are forbidden for SeaweedFS durability" >&2
            exit 78
            ;;
        esac
      done
      ;;
  esac
  case "${compose_command}" in
    config)
      for argument in "${command_tail[@]}"; do
        if [[ "${argument}" =~ ^-[^-]*o ]]; then
          echo "Routine Compose config may not write output or image-lock files" >&2
          exit 78
        fi
        case "${argument}" in
          -o|--output|-o?*|--output=*|--lock-image-digests|--lock-image-digests=*)
            echo "Routine Compose config may not write output or image-lock files" >&2
            exit 78
            ;;
        esac
      done
      ;;
    down|rm)
      for argument in "${command_tail[@]}"; do
        if [[ "${argument}" =~ ^-[^-]*v ]]; then
          echo "Removing the SeaweedFS volume through routine Compose is forbidden" >&2
          exit 78
        fi
        case "${argument}" in
          -v|-v=*|--volumes|--volumes=*)
            echo "Removing the SeaweedFS volume through routine Compose is forbidden" >&2
            exit 78
            ;;
        esac
        if [[ "${compose_command}" == down ]]; then
          case "${argument}" in
            --rmi|--rmi=*)
              echo "Removing the protected SeaweedFS image through routine Compose is forbidden" >&2
              exit 78
              ;;
          esac
        fi
      done
      if [[ "${compose_command}" == rm ]]; then
        parse_mutating_service_targets rm "${command_tail[@]}"
      fi
      ;;
    attach|commit|export|run|exec)
      parse_one_off_service "${compose_command}" "${command_tail[@]}"
      ;;
    cp) parse_cp_endpoints "${command_tail[@]}" ;;
    create)
      echo "Routine Compose create is forbidden; use the validated up lifecycle" >&2
      exit 78
      ;;
    watch)
      echo "Routine Compose watch is forbidden because it can create or recreate service dependencies" >&2
      exit 78
      ;;
    wait)
      echo "Routine Compose wait is forbidden because --down-project can stop the protected project" >&2
      exit 78
      ;;
    kill|pause|start|stop|restart|scale|unpause)
      parse_mutating_service_targets "${compose_command}" "${command_tail[@]}"
      ;;
  esac
fi
case "${compose_command}" in
  config|events|images|logs|ls|port|ps|stats|top|version|volumes) read_only_command=1 ;;
  *) read_only_command=0 ;;
esac
case "${compose_command}" in
  up|run|start|restart|unpause|scale) start_capable_command=1 ;;
esac
if ((start_capable_command)) || [[ "${compose_command}" == down ]]; then
  runtime_boundary_command=1
fi
if ((!read_only_command)); then
  # shellcheck source=seaweedfs_lifecycle_lock.sh
  source "${repo_root}/scripts/seaweedfs_lifecycle_lock.sh"
  acquire_seaweedfs_lifecycle_lock
fi
state_file="${SEAWEEDFS_TOPOLOGY_STATE_FILE:-/var/lib/data-platform-football/seaweedfs-topology.mode}"
compose_files=(-f "${repo_root}/compose.yaml")
state_active=0

if [[ "${state_file}" != /* ]]; then
  echo "SEAWEEDFS_TOPOLOGY_STATE_FILE must be an absolute host path" >&2
  exit 78
fi
state_dir="$(dirname "${state_file}")"
if [[ ! -d "${state_dir}" ]] || [[ -L "${state_dir}" ]]; then
  echo "SeaweedFS topology state directory must be pre-provisioned" >&2
  exit 78
fi
state_dir_mode="$(stat -c '%a' "${state_dir}")"
state_dir_owner="$(stat -c '%u' "${state_dir}")"
if (( (8#${state_dir_mode} & 022) != 0 )) ||
   [[ "${state_dir_owner}" != "0" && "${state_dir_owner}" != "$(id -u)" ]]; then
  echo "SeaweedFS topology state directory is not host-protected: ${state_dir}" >&2
  exit 78
fi
if [[ -e "${state_file}" || -L "${state_file}" ]]; then
  if [[ ! -f "${state_file}" ]] || [[ -L "${state_file}" ]]; then
    echo "Invalid SeaweedFS topology state: ${state_file}" >&2
    exit 78
  fi
  state_mode="$(stat -c '%a' "${state_file}")"
  state_owner="$(stat -c '%u' "${state_file}")"
  if (( (8#${state_mode} & 022) != 0 )) ||
     [[ "${state_owner}" != "0" && "${state_owner}" != "$(id -u)" ]]; then
    echo "SeaweedFS topology state is not host-protected: ${state_file}" >&2
    exit 78
  fi
  if ! state_values="$(python3 - "${state_file}" <<'PY'
import json
import re
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if set(payload) != {
    "schema_version", "mode", "volume_name", "image_id", "inventory_sha256",
    "volume_size_limit_mb"
}:
    raise SystemExit("unexpected topology state fields")
if payload["schema_version"] != 2 or payload["mode"] not in {
    "supervised-v1", "supervised-verification-pending-v1"
}:
    raise SystemExit("unexpected topology state mode")
volume = payload["volume_name"]
image_id = payload["image_id"]
inventory = payload["inventory_sha256"]
volume_size_limit_mb = payload["volume_size_limit_mb"]
if not isinstance(volume, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", volume) is None:
    raise SystemExit("invalid topology volume")
if not isinstance(image_id, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None:
    raise SystemExit("invalid topology image")
if not isinstance(inventory, str) or re.fullmatch(r"[0-9a-f]{64}", inventory) is None:
    raise SystemExit("invalid topology inventory")
if (
    not isinstance(volume_size_limit_mb, int)
    or isinstance(volume_size_limit_mb, bool)
    or not 1 <= volume_size_limit_mb <= 1048576
):
    raise SystemExit("invalid topology volume size limit")
print(
    f"{payload['mode']}\t{volume}\t{image_id}\t{inventory}"
    f"\t{volume_size_limit_mb}"
)
PY
)"; then
    echo "Invalid SeaweedFS topology state: ${state_file}" >&2
    exit 78
  fi
  IFS=$'\t' read -r state_mode state_volume state_image_id \
    state_inventory_sha256 state_volume_size_limit_mb <<<"${state_values}"
  if [[ "${state_mode}" == "supervised-verification-pending-v1" ]]; then
    echo "SeaweedFS cutover verification is pending; routine Compose is locked" >&2
    exit 78
  fi
  if [[ -n "${SEAWEEDFS_DATA_VOLUME_NAME:-}" ]] &&
     [[ "${SEAWEEDFS_DATA_VOLUME_NAME}" != "${state_volume}" ]]; then
    echo "SEAWEEDFS_DATA_VOLUME_NAME differs from protected topology state" >&2
    exit 78
  fi
  export SEAWEEDFS_DATA_VOLUME_NAME="${state_volume}"
  export SEAWEEDFS_EXPECTED_INVENTORY_SHA256="${state_inventory_sha256}"
  export SEAWEEDFS_VOLUME_SIZE_LIMIT_MB="${state_volume_size_limit_mb}"
  compose_files+=(-f "${repo_root}/compose.seaweedfs-supervised.yaml")
  state_active=1
fi

# A caller-supplied COMPOSE_FILE could silently drop the safety overlay.
unset COMPOSE_FILE
if ((state_active)); then
  reject_protected_one_off_volume "${state_volume}"
  if [[ "${compose_command}" == up ]]; then
    if ((up_has_no_recreate && up_has_force_recreate)); then
      echo "Routine Compose up cannot combine --no-recreate and --force-recreate" >&2
      exit 78
    fi
    if ((!up_has_no_recreate)) &&
       ((!up_has_no_deps || up_target_count == 0)); then
      echo "Routine Compose up must use --no-recreate, or target only non-storage services with --no-deps" >&2
      exit 78
    fi
  fi
  if ((start_capable_command)); then
    audit_protected_volume_consumers \
      "${state_volume}" seaweedfs-master seaweedfs-volume seaweedfs-filer
  fi
fi
if ((!read_only_command)); then
  audit_runtime_preconditions
fi
if ((state_active && start_capable_command)) &&
   docker inspect seaweedfs-s3 >/dev/null 2>&1; then
  echo "Legacy SeaweedFS S3 gateway survives protected topology state; recovery is required" >&2
  exit 78
fi

if ((!state_active && !read_only_command)); then
  # Releases before the protected-state wrapper ran the four SeaweedFS planes
  # directly.  Treating such an installation as legacy mini would orphan (or
  # remove) its control/filer/volume containers.  Adoption is deliberately not
  # inferred from mutable Docker metadata: any evidence blocks routine
  # mutation until a separately reviewed adoption/recovery workflow exists.
  for container in seaweedfs-master seaweedfs-volume seaweedfs-filer; do
    if docker inspect "${container}" >/dev/null 2>&1; then
      echo "Existing supervised SeaweedFS container ${container} has no protected topology state; adoption is required" >&2
      exit 78
    fi
  done
  if ! rendered_legacy_values="$(
    docker compose "${compose_global_args[@]}" \
      -f "${repo_root}/compose.yaml" config --format json |
      python3 -c '
import json
import re
import sys

payload = json.load(sys.stdin)
value = payload.get("volumes", {}).get("seaweedfs_data", {}).get("name")
if not isinstance(value, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value) is None:
    raise SystemExit("rendered SeaweedFS volume name is invalid")
service = payload.get("services", {}).get("seaweedfs", {})
command = service.get("command")
entrypoint = service.get("entrypoint")
if not isinstance(command, list) or not command or command[0] != "mini":
    raise SystemExit("rendered legacy SeaweedFS command is invalid")
if entrypoint != ["/usr/local/bin/seaweedfs-legacy-entrypoint"]:
    raise SystemExit("rendered legacy SeaweedFS entrypoint is invalid")
hardening_flags = {
    "-s3=false",
    "-s3.port.iceberg=0",
    "-s3.iam=false",
    "-webdav=false",
    "-admin.ui=false",
    "-filer.disableDirListing=true",
    "-filer.ui.deleteDir=false",
}
if not all(command.count(flag) == 1 for flag in hardening_flags):
    raise SystemExit("rendered legacy SeaweedFS isolation flags are invalid")
buckets = [item.removeprefix("-bucket=") for item in command if item.startswith("-bucket=")]
if len(buckets) != 1 or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", buckets[0]) is None:
    raise SystemExit("rendered legacy SeaweedFS bucket is invalid")
pre_isolation_command = [item for item in command if item not in hardening_flags]
gateway = payload.get("services", {}).get("seaweedfs-s3", {})
gateway_command = gateway.get("command")
gateway_entrypoint = gateway.get("entrypoint")
gateway_tmpfs = gateway.get("tmpfs")
if (
    not isinstance(gateway_command, list)
    or not gateway_command
    or gateway_command[0] != "s3"
    or "-filer=seaweedfs:8888" not in gateway_command
    or "-port.iceberg=0" not in gateway_command
    or "-iam=false" not in gateway_command
):
    raise SystemExit("rendered legacy SeaweedFS S3 gateway is invalid")
if gateway_entrypoint != ["/usr/local/bin/seaweedfs-legacy-entrypoint"]:
    raise SystemExit("rendered legacy SeaweedFS S3 entrypoint is invalid")
if gateway_tmpfs != ["/data:rw,noexec,nosuid,nodev,size=16m,mode=0700"]:
    raise SystemExit("rendered legacy SeaweedFS S3 tmpfs is invalid")
print(value)
print(json.dumps(command, separators=(",", ":")))
print(json.dumps(entrypoint, separators=(",", ":")))
print(json.dumps(pre_isolation_command, separators=(",", ":")))
print(buckets[0])
print(json.dumps(gateway_command, separators=(",", ":")))
'
  )"; then
    echo "Cannot render the exact legacy SeaweedFS identity before a state-less mutation" >&2
    exit 78
  fi
  mapfile -t rendered_legacy_identity <<<"${rendered_legacy_values}"
  if ((${#rendered_legacy_identity[@]} != 6)); then
    echo "Cannot parse the exact legacy SeaweedFS identity before a state-less mutation" >&2
    exit 78
  fi
  rendered_legacy_volume="${rendered_legacy_identity[0]}"
  rendered_legacy_command="${rendered_legacy_identity[1]}"
  rendered_legacy_entrypoint="${rendered_legacy_identity[2]}"
  rendered_pre_isolation_command="${rendered_legacy_identity[3]}"
  rendered_legacy_bucket="${rendered_legacy_identity[4]}"
  rendered_gateway_command="${rendered_legacy_identity[5]}"
  if [[ "${rendered_legacy_volume}" != "${SEAWEEDFS_LEGACY_VOLUME}" ]]; then
    echo "State-less legacy SeaweedFS must retain volume ${SEAWEEDFS_LEGACY_VOLUME}; adoption is required for any other name" >&2
    exit 78
  fi
  # Compose re-reads caller env files for the final command. Shell environment
  # has higher interpolation precedence, so freeze the exact audited resource
  # and prevent an atomic env-file swap from redirecting `up` after validation.
  export SEAWEEDFS_DATA_VOLUME_NAME="${rendered_legacy_volume}"
  export ICEBERG_WAREHOUSE="${rendered_legacy_bucket}"
  reject_protected_one_off_volume "${rendered_legacy_volume}"

  audit_image_id="$({
    docker image inspect --format '{{.Id}}' "${SEAWEEDFS_IMAGE}" 2>/dev/null || true
  })"
  if [[ ! "${audit_image_id}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "Pinned SeaweedFS audit image is absent; run: docker pull ${SEAWEEDFS_IMAGE}" >&2
    exit 78
  fi
  if docker inspect seaweedfs >/dev/null 2>&1; then
    if ! existing_legacy_command="$(
      docker inspect --format '{{json .Config.Cmd}}' seaweedfs
    )" || ! existing_legacy_entrypoint="$(
      docker inspect --format '{{json .Config.Entrypoint}}' seaweedfs
    )" || ! existing_legacy_volume="$(
      docker inspect --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}' seaweedfs
    )" || ! existing_legacy_image_id="$(
      docker inspect --format '{{.Image}}' seaweedfs
    )" || ! existing_legacy_image_ref="$(
      docker inspect --format '{{.Config.Image}}' seaweedfs
    )" || ! existing_legacy_networks="$(
      docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' seaweedfs | sed '/^$/d' | LC_ALL=C sort
    )"; then
      echo "Cannot inspect the existing legacy SeaweedFS identity" >&2
      exit 78
    fi
    if [[ "${existing_legacy_volume}" != "${rendered_legacy_volume}" ]]; then
      echo "Existing SeaweedFS container differs from the exact rendered legacy identity; adoption is required" >&2
      exit 78
    fi
    current_legacy_identity=0
    if [[ "${existing_legacy_command}" == "${rendered_legacy_command}" ]] &&
       [[ "${existing_legacy_entrypoint}" == "${rendered_legacy_entrypoint}" ]] &&
       [[ "${existing_legacy_image_id}" == "${audit_image_id}" ]] &&
       [[ "${existing_legacy_image_ref}" == "${SEAWEEDFS_IMAGE}" ]] &&
       [[ "${existing_legacy_networks}" == "${SEAWEEDFS_CONTROL_NETWORK}" ]]; then
      current_legacy_identity=1
    fi
    if ((current_legacy_identity && runtime_boundary_command)); then
      audit_legacy_storage_host_boundary seaweedfs mini
    fi
    known_old_legacy_identity=0
    if [[ "${existing_legacy_command}" == "${rendered_legacy_command}" ||
          "${existing_legacy_command}" == "${rendered_pre_isolation_command}" ]] &&
       [[ "${existing_legacy_entrypoint}" == "${rendered_legacy_entrypoint}" ||
          "${existing_legacy_entrypoint}" == '["/entrypoint.sh"]' ||
          "${existing_legacy_entrypoint}" == null ]] &&
       [[ "${existing_legacy_image_ref}" == "${SEAWEEDFS_LEGACY_IMAGE}" ||
          "${existing_legacy_image_ref}" == "${SEAWEEDFS_IMAGE}" ]] &&
       [[ "${existing_legacy_image_id}" == "${audit_image_id}" ]]; then
      known_old_legacy_identity=1
    fi
    if ((!current_legacy_identity)); then
      allow_old_legacy_identity=0
      case "${compose_command}" in
        build|down|kill|pause|pull|rm|stop) allow_old_legacy_identity=1 ;;
      esac
      if ((!known_old_legacy_identity || !allow_old_legacy_identity)); then
        echo "Existing SeaweedFS runtime identity is stale; adoption is required through the reviewed quiesced isolation rollout" >&2
        exit 78
      fi
    fi
    if ((known_old_legacy_identity && !current_legacy_identity && runtime_boundary_command)); then
      # Compose applies the reviewed 120-second grace period from the current
      # model to this one-time old-container shutdown. Old containers predate
      # persisted StopTimeout metadata, so only their non-destructive signal
      # and restart identity can be attested before replacement.
      audit_storage_stop_boundary seaweedfs ""
    fi
  fi

  if docker inspect seaweedfs-s3 >/dev/null 2>&1; then
    if ! existing_gateway_command="$(
      docker inspect --format '{{json .Config.Cmd}}' seaweedfs-s3
    )" || ! existing_gateway_entrypoint="$(
      docker inspect --format '{{json .Config.Entrypoint}}' seaweedfs-s3
    )" || ! existing_gateway_image_id="$(
      docker inspect --format '{{.Image}}' seaweedfs-s3
    )" || ! existing_gateway_image_ref="$(
      docker inspect --format '{{.Config.Image}}' seaweedfs-s3
    )" || ! existing_gateway_data_mount="$(
      docker inspect --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}' seaweedfs-s3
    )" || ! existing_gateway_networks="$(
      docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' seaweedfs-s3 | sed '/^$/d' | LC_ALL=C sort
    )"; then
      echo "Cannot inspect the existing legacy SeaweedFS S3 gateway identity" >&2
      exit 78
    fi
    if [[ "${existing_gateway_command}" != "${rendered_gateway_command}" ]] ||
       [[ "${existing_gateway_entrypoint}" != "${rendered_legacy_entrypoint}" ]] ||
       [[ "${existing_gateway_image_id}" != "${audit_image_id}" ]] ||
       [[ "${existing_gateway_image_ref}" != "${SEAWEEDFS_IMAGE}" ]] ||
       [[ -n "${existing_gateway_data_mount}" ]] ||
       [[ "${existing_gateway_networks}" != "${SEAWEEDFS_CONTROL_NETWORK}" ]]; then
      echo "Existing SeaweedFS S3 gateway differs from its isolated runtime identity; adoption is required" >&2
      exit 78
    fi
    if ((runtime_boundary_command)); then
      audit_legacy_storage_host_boundary seaweedfs-s3 gateway
    fi
  fi

  candidate_volumes=("${rendered_legacy_volume}")
  if ! labeled_volumes="$({
    docker volume ls \
      --filter label=com.docker.compose.volume=seaweedfs_data \
      --format '{{.Name}}'
  })"; then
    echo "Cannot enumerate prior Compose-managed SeaweedFS volumes" >&2
    exit 78
  fi
  while IFS= read -r volume; do
    [[ -n "${volume}" ]] || continue
    if [[ "${volume}" != "${rendered_legacy_volume}" ]]; then
      echo "Prior Compose-managed SeaweedFS volume ${volume} differs from the rendered legacy volume; adoption is required" >&2
      exit 78
    fi
    candidate_volumes+=("${volume}")
  done <<<"${labeled_volumes}"
  declare -A audited_volumes=()
  for volume in "${candidate_volumes[@]}"; do
    [[ -n "${volume}" ]] || continue
    if [[ ! "${volume}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ ]]; then
      echo "Invalid candidate SeaweedFS volume name during state-less audit" >&2
      exit 78
    fi
    [[ -z "${audited_volumes[${volume}]:-}" ]] || continue
    audited_volumes["${volume}"]=1
    if ! docker volume inspect "${volume}" >/dev/null 2>&1; then
      continue
    fi
    audit_image_id="$({
      docker image inspect --format '{{.Id}}' "${SEAWEEDFS_IMAGE}" 2>/dev/null || true
    })"
    if [[ ! "${audit_image_id}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
      echo "Pinned SeaweedFS audit image is absent; run: docker pull ${SEAWEEDFS_IMAGE}" >&2
      exit 78
    fi
    marker_status=0
    docker run --rm --pull never --read-only --network none \
      --entrypoint /bin/sh -v "${volume}:/data:ro" \
      "${audit_image_id}" -euc '
        marker=/data/.supervised-topology-cutover-approved
        if [ -L "${marker}" ]; then exit 74; fi
        if [ ! -e "${marker}" ]; then exit 3; fi
        test -f "${marker}"
      ' >/dev/null || marker_status=$?
    case "${marker_status}" in
      0)
        echo "SeaweedFS volume ${volume} has a supervised marker but no protected topology state; adoption is required" >&2
        exit 78
        ;;
      3) ;;
      *)
        echo "Cannot safely audit SeaweedFS volume ${volume} for a supervised marker" >&2
        exit 78
        ;;
    esac
  done
  if ((start_capable_command)); then
    audit_protected_volume_consumers "${rendered_legacy_volume}" seaweedfs
  fi
fi

if ((state_active)); then
  for argument in "$@"; do
    case "${argument}" in
      pull|--pull|--pull=*)
        echo "SeaweedFS image pulls require an approved topology-state transition" >&2
        exit 78
        ;;
    esac
  done
  if ! rendered_volume="$(
    docker compose "${compose_files[@]}" "${compose_global_args[@]}" \
      config --format json |
      python3 -c '
import json
import os
import sys

payload = json.load(sys.stdin)
volume_size_limit_mb = os.environ["SEAWEEDFS_VOLUME_SIZE_LIMIT_MB"]
expected = f"-volumeSizeLimitMB={volume_size_limit_mb}"
command = payload["services"]["seaweedfs-master"]["command"]
if command.count(expected) != 1:
    raise SystemExit("rendered master volume size differs from protected state")
print(payload["volumes"]["seaweedfs_data"]["name"])
'
  )" || [[ "${rendered_volume}" != "${state_volume}" ]]; then
    echo "Rendered SeaweedFS topology differs from protected state" >&2
    exit 78
  fi
  runtime_image_id="$(
    docker image inspect --format '{{.Id}}' "${SEAWEEDFS_IMAGE}" 2>/dev/null || true
  )"
  if [[ "${runtime_image_id}" != "${state_image_id}" ]]; then
    echo "SeaweedFS image differs from protected topology state" >&2
    exit 78
  fi
  if ! docker volume inspect "${state_volume}" >/dev/null 2>&1; then
    echo "Protected SeaweedFS volume does not exist" >&2
    exit 78
  fi
  for container in seaweedfs seaweedfs-master seaweedfs-volume seaweedfs-filer; do
    container_image_id="$(
      docker inspect --format '{{.Image}}' "${container}" 2>/dev/null || true
    )"
    if [[ -n "${container_image_id}" ]] &&
       [[ "${container_image_id}" != "${state_image_id}" ]]; then
      echo "${container} image differs from protected topology state" >&2
      exit 78
    fi
  done
  if ((runtime_boundary_command)); then
    for container in seaweedfs seaweedfs-master seaweedfs-volume seaweedfs-filer; do
      if docker inspect "${container}" >/dev/null 2>&1; then
        audit_supervised_runtime_container "${container}"
      fi
    done
  fi
  for container in seaweedfs-master seaweedfs-volume seaweedfs-filer; do
    if docker inspect "${container}" >/dev/null 2>&1; then
      mounted_volume="$(
        docker inspect --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}' "${container}"
      )"
      if [[ "${mounted_volume}" != "${state_volume}" ]]; then
        echo "${container} data mount differs from protected topology state" >&2
        exit 78
      fi
    fi
  done
  if docker inspect seaweedfs >/dev/null 2>&1; then
    gateway_data_mount="$(
      docker inspect --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}' seaweedfs
    )"
    if [[ -n "${gateway_data_mount}" ]]; then
      echo "Supervised S3 gateway must not mount the storage volume" >&2
      exit 78
    fi
  fi
  expected_marker="full-bucket-inventory-v2:${state_inventory_sha256}"
  if ! docker run --rm --read-only --network none --entrypoint /bin/sh \
    -v "${state_volume}:/data:ro" "${state_image_id}" -euc \
    'test ! -L /data/.supervised-topology-cutover-approved
     test -f /data/.supervised-topology-cutover-approved
     test "$(cat /data/.supervised-topology-cutover-approved)" = "$1"
     test ! -L /data/mini.options
     test -f /data/mini.options
     test "$(grep -c "master[.]volumeSizeLimitMB" /data/mini.options || true)" = 1
     test "$(grep "^master[.]volumeSizeLimitMB=[1-9][0-9]*$" /data/mini.options)" = "master.volumeSizeLimitMB=$2"' \
    _ "${expected_marker}" "${state_volume_size_limit_mb}"; then
    echo "SeaweedFS volume marker differs from protected topology state" >&2
    exit 78
  fi
  destructive_command=0
  destructive_volumes=0
  for argument in "$@"; do
    case "${argument}" in
      down|rm) destructive_command=1 ;;
      -v|-v=*|--volumes|--volumes=*) destructive_volumes=1 ;;
    esac
  done
  if ((destructive_command && destructive_volumes)); then
    echo "Removing the protected SeaweedFS volume is forbidden" >&2
    exit 78
  fi
fi
if [[ "${compose_command}" == up ]] && ((!state_active)); then
  if ((up_has_no_recreate && up_has_force_recreate)); then
    echo "Routine Compose up cannot combine --no-recreate and --force-recreate" >&2
    exit 78
  fi
  if ((!up_has_no_recreate)) &&
     ((!up_has_no_deps || up_target_count == 0)); then
    echo "Routine Compose up must use --no-recreate, or target only non-storage services with --no-deps" >&2
    exit 78
  fi
fi
validate_one_off_volume_specs
exec docker compose "${compose_files[@]}" "$@"
