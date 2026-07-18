#!/usr/bin/env bash
# Shared, non-truncating lifecycle lock for topology-changing host commands.

acquire_seaweedfs_lifecycle_lock() {
  local lock_file lock_dir lock_mode lock_owner lock_dir_owner
  local opened_identity path_identity
  lock_file="${SEAWEEDFS_CUTOVER_LOCK_FILE:-/var/lib/data-platform-football/seaweedfs-topology.lock}"
  lock_dir="$(dirname "${lock_file}")"
  if [[ "${lock_file}" != /* ]] || [[ ! -d "${lock_dir}" ]] ||
     [[ -L "${lock_dir}" ]]; then
    echo "SeaweedFS lifecycle lock directory must be pre-provisioned" >&2
    return 73
  fi
  lock_mode="$(stat -c '%a' "${lock_dir}")"
  lock_dir_owner="$(stat -c '%u' "${lock_dir}")"
  if (( (8#${lock_mode} & 022) != 0 )) ||
     [[ "${lock_dir_owner}" != "0" && "${lock_dir_owner}" != "$(id -u)" ]]; then
    echo "SeaweedFS lifecycle lock directory is not host-protected" >&2
    return 73
  fi
  if [[ ! -f "${lock_file}" ]] || [[ -L "${lock_file}" ]]; then
    echo "SeaweedFS lifecycle lock file must be a pre-provisioned regular file" >&2
    return 73
  fi
  lock_mode="$(stat -c '%a' "${lock_file}")"
  lock_owner="$(stat -c '%u' "${lock_file}")"
  # The lock is opened read-only, so read permission is enough to acquire an
  # exclusive flock. Require 0600-style least privilege so only the deployment
  # identity can accidentally contend with cutover/recovery. All host accounts
  # remain trusted storage principals as documented by the runbook.
  if (( (8#${lock_mode} & 077) != 0 )) ||
     [[ "${lock_owner}" != "0" && "${lock_owner}" != "$(id -u)" ]]; then
    echo "SeaweedFS lifecycle lock file is not host-protected" >&2
    return 73
  fi
  # Open read-only: acquiring the lock can never truncate a planted target.
  exec 9<"${lock_file}"
  # Bind the validated pathname to the inode actually used by flock. The
  # protected parent prevents pathname replacement by a different local
  # identity; the identity check also fails closed if the path changed during
  # acquisition. This is defense in depth, not a hostile-host security boundary.
  if ! opened_identity="$(stat -Lc '%d:%i:%a:%u' /proc/self/fd/9)" ||
     ! path_identity="$(stat -Lc '%d:%i:%a:%u' "${lock_file}")" ||
     [[ "${opened_identity}" != "${path_identity}" ]]; then
    exec 9<&-
    echo "SeaweedFS lifecycle lock changed during acquisition" >&2
    return 73
  fi
  if ! flock -n 9; then
    echo "Another SeaweedFS lifecycle operation is already running" >&2
    return 73
  fi
}
