#!/bin/sh
# Refuse to open legacy SeaweedFS files until the backup-gated cutover approves it.
set -eu

data_root="${SEAWEEDFS_DATA_ROOT:-/data}"
marker="${data_root}/.supervised-topology-cutover-approved"
image_entrypoint="${SEAWEEDFS_IMAGE_ENTRYPOINT:-/entrypoint.sh}"
expected_inventory="${SEAWEEDFS_EXPECTED_INVENTORY_SHA256:-}"
fresh_transition="${SEAWEEDFS_ALLOW_FRESH_TRANSITION:-}"
volume_size_limit_mb="${SEAWEEDFS_VOLUME_SIZE_LIMIT_MB:-}"
mini_options="${data_root}/mini.options"

case "${volume_size_limit_mb}" in
  ''|*[!0-9]*|0)
    echo "Protected SeaweedFS volume size limit is required" >&2
    exit 78
    ;;
esac
if [ "${volume_size_limit_mb}" -gt 1048576 ]; then
  echo "Protected SeaweedFS volume size limit is invalid" >&2
  exit 78
fi

validate_mini_options() {
  if [ ! -f "${mini_options}" ] || [ -L "${mini_options}" ] ||
     [ "$(grep -c 'master[.]volumeSizeLimitMB' "${mini_options}" || true)" != 1 ] ||
     [ "$(grep '^master[.]volumeSizeLimitMB=[1-9][0-9]*$' "${mini_options}" || true)" != "master.volumeSizeLimitMB=${volume_size_limit_mb}" ]; then
    echo "SeaweedFS mini.options differs from protected state" >&2
    exit 78
  fi
}

if [ -e "${marker}" ] || [ -L "${marker}" ]; then
  if [ ! -f "${marker}" ] || [ -L "${marker}" ]; then
    echo "SeaweedFS topology marker must be a regular non-symlink file" >&2
    exit 78
  fi
  marker_value="$(cat "${marker}")"
  case "${marker_value}" in
    fresh-supervised-volume-v1)
      if [ "${fresh_transition}" != "restore-empty-volume-v1" ]; then
        echo "Fresh volume requires an explicit recovery transition" >&2
        exit 78
      fi
      validate_mini_options
      ;;
    full-bucket-inventory-v2:*)
      digest=${marker_value#full-bucket-inventory-v2:}
      if ! printf '%s\n' "${digest}" | grep -Eq '^[0-9a-f]{64}$' ||
         [ -z "${expected_inventory}" ] ||
         [ "${digest}" != "${expected_inventory}" ]; then
        echo "SeaweedFS inventory marker differs from protected state" >&2
        exit 78
      fi
      validate_mini_options
      ;;
    *)
      echo "SeaweedFS topology marker has invalid authority content" >&2
      exit 78
      ;;
  esac
else
  # A genuinely empty named volume is safe for a fresh supervised deployment.
  # Any pre-existing entry belongs to weed mini (or is otherwise unaudited),
  # so generic compose-up must fail before opening Raft/LevelDB/volume files.
  if find "${data_root}" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
    echo "SeaweedFS legacy data requires the backup-gated cutover script" >&2
    exit 78
  fi
  if [ "${fresh_transition}" != "restore-empty-volume-v1" ]; then
    echo "Empty volume requires an explicit recovery transition" >&2
    exit 78
  fi
  umask 077
  printf '%s\n' 'fresh-supervised-volume-v1' >"${marker}"
  printf '%s\n' \
    '#!/bin/bash' \
    '# Recovery-pinned mini allocation policy; do not edit.' \
    "master.volumeSizeLimitMB=${volume_size_limit_mb}" >"${mini_options}"
  sync
fi

exec "${image_entrypoint}" "$@"
