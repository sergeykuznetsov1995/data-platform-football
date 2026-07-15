#!/bin/sh
# Refuse to open legacy SeaweedFS files until the backup-gated cutover approves it.
set -eu

data_root="${SEAWEEDFS_DATA_ROOT:-/data}"
marker="${data_root}/.supervised-topology-cutover-approved"
image_entrypoint="${SEAWEEDFS_IMAGE_ENTRYPOINT:-/entrypoint.sh}"

if [ ! -s "${marker}" ]; then
  # A genuinely empty named volume is safe for a fresh supervised deployment.
  # Any pre-existing entry belongs to weed mini (or is otherwise unaudited),
  # so generic compose-up must fail before opening Raft/LevelDB/volume files.
  if find "${data_root}" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
    echo "SeaweedFS legacy data requires the backup-gated cutover script" >&2
    exit 78
  fi
  umask 077
  printf '%s\n' 'fresh-supervised-volume-v1' >"${marker}"
fi

exec "${image_entrypoint}" "$@"
