#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
requested_revision="${1:-HEAD}"
image_repository="${2:-data-platform-fbref-acceptance}"
runtime_base="${FBREF_RUNTIME_BASE:?set a reviewed scheduler runtime image}"

git_sha="$(git -C "$repo_root" rev-parse --verify "${requested_revision}^{commit}")"
git_tree="$(git -C "$repo_root" rev-parse --verify "${git_sha}^{tree}")"
if [[ ! "$git_sha" =~ ^[0-9a-f]{40}$ || ! "$git_tree" =~ ^[0-9a-f]{40}$ ]]; then
  echo "full Git SHA/tree are required" >&2
  exit 2
fi

runtime_base_id="$(docker image inspect --format '{{.Id}}' "$runtime_base")"
if [[ ! "$runtime_base_id" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "FBREF_RUNTIME_BASE did not resolve to one immutable image ID" >&2
  exit 2
fi

build_context="$(mktemp -d -t fbref-acceptance-build-XXXXXXXX)"
base_build_ref="local/fbref-acceptance-base:${runtime_base_id#sha256:}-${BASHPID}"
cleanup() {
  docker image rm "$base_build_ref" >/dev/null 2>&1 || true
  rm -rf -- "$build_context"
}
trap cleanup EXIT

# Dockerfile FROM does not accept a raw local `sha256:<image-id>` reference.
# Give that exact ID a process-unique temporary tag, then prove the tag still
# resolves to the inspected ID before and after the build.
docker image tag "$runtime_base_id" "$base_build_ref"
if [[ "$(docker image inspect --format '{{.Id}}' "$base_build_ref")" != \
  "$runtime_base_id" ]]; then
  echo "temporary FBref runtime base tag changed before build" >&2
  exit 2
fi

git -C "$repo_root" archive --format=tar "$git_sha" \
  dags scrapers scripts configs >"$build_context/source.tar"
git -C "$repo_root" show \
  "${git_sha}:docker/images/airflow/Dockerfile.fbref-acceptance" \
  >"$build_context/Dockerfile"
source_sha256="$(sha256sum "$build_context/source.tar" | awk '{print $1}')"
if [[ ! "$source_sha256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "could not seal FBref acceptance source archive" >&2
  exit 2
fi

image_ref="${image_repository}:${git_sha}"
docker build --pull=false \
  --build-arg "FBREF_RUNTIME_BASE=${base_build_ref}" \
  --build-arg "FBREF_ACCEPTANCE_GIT_SHA=${git_sha}" \
  --build-arg "FBREF_ACCEPTANCE_GIT_TREE=${git_tree}" \
  --build-arg "FBREF_ACCEPTANCE_SOURCE_SHA256=${source_sha256}" \
  --label "org.opencontainers.image.base.digest=${runtime_base_id}" \
  --tag "$image_ref" \
  "$build_context" >&2

if [[ "$(docker image inspect --format '{{.Id}}' "$base_build_ref")" != \
  "$runtime_base_id" ]]; then
  echo "temporary FBref runtime base tag changed during build" >&2
  exit 2
fi

image_id="$(docker image inspect --format '{{.Id}}' "$image_ref")"
label_sha="$(docker image inspect \
  --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
  "$image_ref")"
if [[ ! "$image_id" =~ ^sha256:[0-9a-f]{64}$ || "$label_sha" != "$git_sha" ]]; then
  echo "built image provenance does not match requested commit" >&2
  exit 2
fi

printf 'FBREF_ACCEPTANCE_GIT_SHA=%s\n' "$git_sha"
printf 'FBREF_ACCEPTANCE_AIRFLOW_IMAGE=%s\n' "$image_id"
printf 'FBREF_ACCEPTANCE_TAG=%s\n' "$image_ref"
printf 'FBREF_ACCEPTANCE_SOURCE_SHA256=%s\n' "$source_sha256"
