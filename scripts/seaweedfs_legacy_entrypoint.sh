#!/usr/bin/env sh
set -eu

data_dir=${SEAWEEDFS_DATA_DIR:-/data}
marker=${data_dir}/.supervised-topology-cutover-approved
if [ "${1:-}" = "mini" ] && { [ -e "${marker}" ] || [ -L "${marker}" ]; }; then
  # The protected wrapper and overlay are the only authority that may expose
  # the post-cutover S3 gateway.  A raw base Compose invocation retains the
  # legacy /data mount and capabilities, so even a valid marker must abort
  # instead of silently rewriting mini into an unvalidated gateway.
  echo "Legacy weed mini is disabled on every supervised-marked volume" >&2
  exit 78
fi

exec weed "$@"
