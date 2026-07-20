#!/usr/bin/env bash
# Issue one exact scheduled WhoScored approval without exposing signing keys to
# Airflow. Installed as /usr/local/libexec/whoscored-daily-issuer by the runbook.
set -Eeuo pipefail
export PATH=/usr/bin:/bin

readonly DOCKER=/usr/bin/docker
readonly PYTHON=/usr/bin/python3
readonly JQ=/usr/bin/jq
readonly DATE=/usr/bin/date
readonly SHA256SUM=/usr/bin/sha256sum
readonly FLOCK=/usr/bin/flock
readonly PLANNER_NETWORK=dp-backend
readonly MAX_SCOPES=3
readonly TOTAL_BYTES=135000000
readonly WINDOW_START=0900
readonly WINDOW_END=0930

fail() {
  echo "WhoScored daily issuer blocked: $*" >&2
  exit 78
}

require_variable() {
  local name="$1"
  test -n "${!name:-}" || fail "$name is required"
}

require_canonical_path() {
  local path="$1"
  test "${path:0:1}" = / || fail "path must be absolute: $path"
  test -e "$path" || fail "path is missing: $path"
  test "$(readlink -f -- "$path")" = "$path" || fail "path is not canonical: $path"
}

require_private_directory() {
  local path="$1" identity
  require_canonical_path "$path"
  test -d "$path" && test ! -L "$path" || fail "not a directory: $path"
  identity="$(stat -c '%u:%g:%a' -- "$path")"
  case "$identity" in
    50000:0:700|50000:0:750) ;;
    *) fail "directory must be owned by 50000:0 and private: $path ($identity)" ;;
  esac
}

require_private_file() {
  local path="$1" identity
  require_canonical_path "$path"
  test -f "$path" && test ! -L "$path" || fail "not a regular file: $path"
  identity="$(stat -c '%u:%g:%a:%h' -- "$path")"
  case "$identity" in
    0:0:400:1|0:0:600:1|50000:0:400:1|50000:0:600:1) ;;
    *) fail "file is not a private single-link authority input: $path ($identity)" ;;
  esac
}

require_root_private_file() {
  local path="$1" identity
  require_canonical_path "$path"
  test -f "$path" && test ! -L "$path" || fail "not a regular file: $path"
  identity="$(stat -c '%u:%g:%a:%h' -- "$path")"
  case "$identity" in
    0:0:400:1|0:0:600:1) ;;
    *) fail "authority input must be root-owned, private, and single-link: $path ($identity)" ;;
  esac
}

require_root_private_directory() {
  local path="$1" identity
  require_canonical_path "$path"
  test -d "$path" && test ! -L "$path" || fail "not a directory: $path"
  identity="$(stat -c '%u:%g:%a' -- "$path")"
  test "$identity" = 0:0:700 || \
    fail "runtime directory must be root:root 0700: $path ($identity)"
}

require_container_private_file() {
  local path="$1" identity
  require_canonical_path "$path"
  test -f "$path" && test ! -L "$path" || fail "not a regular file: $path"
  identity="$(stat -c '%u:%g:%a:%h' -- "$path")"
  case "$identity" in
    50000:0:400:1|50000:0:600:1) ;;
    *) fail "container input must be owned by 50000:0 and private: $path ($identity)" ;;
  esac
}

require_frozen_container_file() {
  local path="$1" identity
  require_canonical_path "$path"
  test -f "$path" && test ! -L "$path" || fail "not a regular file: $path"
  identity="$(stat -c '%u:%g:%a:%h' -- "$path")"
  test "$identity" = 0:0:440:1 || \
    fail "frozen container input must be root:root 0440 and single-link: $path ($identity)"
}

require_digest_image() {
  local value="$1"
  [[ "$value" =~ ^[^[:space:]@]+@sha256:[0-9a-f]{64}$ ]] || \
    fail "image is not pinned by one sha256 digest: $value"
}

for variable in \
  WHOSCORED_RELEASE_ROOT \
  WHOSCORED_PLANNER_IMAGE \
  WHOSCORED_SIGNER_IMAGE \
  WHOSCORED_DEPLOYMENT_ATTESTATION_FILE \
  WHOSCORED_COMMON_DIGEST_OVERRIDE \
  WHOSCORED_GATEWAY_DIGEST_OVERRIDE \
  WHOSCORED_COMPOSE_ENV_FILE \
  WHOSCORED_RUNTIME_ENV_FILE \
  WHOSCORED_PROXY_POOL_ENV_FILE \
  WHOSCORED_PLANNER_ENV_FILE \
  WHOSCORED_COHORT_FILE \
  WHOSCORED_PROVIDER_POLICY_FILE \
  WHOSCORED_DEPLOYMENT_ADMISSION_RECEIPT_FILE \
  WHOSCORED_CHARTER_FILE \
  WHOSCORED_PROXY_APPROVAL_HOST_DIR \
  WHOSCORED_SCHEDULED_PAID_POINTER_HOST_DIR \
  WHOSCORED_ISSUANCE_LEDGER_HOST_DIR \
  WHOSCORED_RUNTIME_SHA256 \
  WHOSCORED_CLASSIFIER_SHA256 \
  RUNTIME_DIRECTORY \
  CREDENTIALS_DIRECTORY
do
  require_variable "$variable"
done

test "$(id -u)" = 0 || fail "wrapper requires root for the protected Docker socket"
test -x "$DOCKER" && test -x "$PYTHON" && test -x "$JQ" && \
  test -S /run/docker.sock || fail "trusted admission boundary is unavailable"
require_digest_image "$WHOSCORED_PLANNER_IMAGE"
require_digest_image "$WHOSCORED_SIGNER_IMAGE"
[[ "$WHOSCORED_RUNTIME_SHA256" =~ ^[0-9a-f]{64}$ ]] || fail "runtime digest is invalid"
[[ "$WHOSCORED_CLASSIFIER_SHA256" =~ ^[0-9a-f]{64}$ ]] || fail "classifier digest is invalid"

require_canonical_path "$WHOSCORED_RELEASE_ROOT"
test -e "$WHOSCORED_RELEASE_ROOT/.git" || fail "release root has no Git worktree identity"
for relative in dags scripts scrapers configs/medallion; do
  require_canonical_path "$WHOSCORED_RELEASE_ROOT/$relative"
done
require_root_private_directory "$RUNTIME_DIRECTORY"
for path in \
  "$WHOSCORED_DEPLOYMENT_ATTESTATION_FILE" \
  "$WHOSCORED_COMMON_DIGEST_OVERRIDE" \
  "$WHOSCORED_GATEWAY_DIGEST_OVERRIDE" \
  "$WHOSCORED_COMPOSE_ENV_FILE" \
  "$WHOSCORED_RUNTIME_ENV_FILE" \
  "$WHOSCORED_PROXY_POOL_ENV_FILE" \
  "$WHOSCORED_PLANNER_ENV_FILE" \
  "$WHOSCORED_COHORT_FILE" \
  "$WHOSCORED_PROVIDER_POLICY_FILE" \
  "$WHOSCORED_DEPLOYMENT_ADMISSION_RECEIPT_FILE" \
  "$WHOSCORED_CHARTER_FILE"
do
  require_root_private_file "$path"
done
for path in \
  "$WHOSCORED_PROXY_APPROVAL_HOST_DIR" \
  "$WHOSCORED_SCHEDULED_PAID_POINTER_HOST_DIR" \
  "$WHOSCORED_ISSUANCE_LEDGER_HOST_DIR"
do
  require_private_directory "$path"
done

if grep -Eq '^[[:space:]]*(WHOSCORED_PROXY_(APPROVAL|OWNER|ISSUANCE_LEDGER)_HMAC_SECRET)=' \
  "$WHOSCORED_PLANNER_ENV_FILE"; then
  fail "planner env file contains a signing credential"
fi

for name in approval-hmac owner-hmac issuance-ledger-hmac; do
  require_private_file "$CREDENTIALS_DIRECTORY/$name"
done

readonly LOCK_PATH="$RUNTIME_DIRECTORY/issuer.lock"
exec {lock_fd}>"$LOCK_PATH"
"$FLOCK" --exclusive --nonblock "$lock_fd" || fail "another issuer is active"

readonly utc_hhmm="$($DATE -u '+%H%M')"
[[ "$utc_hhmm" =~ ^[0-9]{4}$ ]] || fail "UTC wall clock is invalid"
if ((10#$utc_hhmm < 10#$WINDOW_START || 10#$utc_hhmm > 10#$WINDOW_END)); then
  fail "issuer may run only from 09:00 through 09:30 UTC"
fi

readonly running_admission_receipt="$RUNTIME_DIRECTORY/running-admission.json"
test ! -e "$running_admission_receipt" || fail "running admission receipt already exists"
admission_clean=(
  env -i
  HOME=/nonexistent
  LANG=C.UTF-8
  LC_ALL=C.UTF-8
  PATH=/usr/bin:/bin
  "$PYTHON" -I -S
  "$WHOSCORED_RELEASE_ROOT/scripts/whoscored_production_admission.py"
)
"${admission_clean[@]}" verify-running \
  --root "$WHOSCORED_RELEASE_ROOT" \
  --deployment-attestation "$WHOSCORED_DEPLOYMENT_ATTESTATION_FILE" \
  --common-override "$WHOSCORED_COMMON_DIGEST_OVERRIDE" \
  --gateway-override "$WHOSCORED_GATEWAY_DIGEST_OVERRIDE" \
  --env-file "$WHOSCORED_COMPOSE_ENV_FILE" \
  --env-file "$WHOSCORED_RUNTIME_ENV_FILE" \
  --env-file "$WHOSCORED_PROXY_POOL_ENV_FILE" \
  --provider-policy "$WHOSCORED_PROVIDER_POLICY_FILE" \
  --owner-secret-file "$CREDENTIALS_DIRECTORY/owner-hmac" \
  --deployment-admission-receipt "$WHOSCORED_DEPLOYMENT_ADMISSION_RECEIPT_FILE" \
  --service airflow-scheduler \
  --service flaresolverr \
  --service flaresolverr_whoscored_paid \
  --service whoscored_paid_gateway \
  --service whoscored_proxy_filter \
  >"$running_admission_receipt"
require_root_private_file "$running_admission_receipt"
"$JQ" -e \
  --arg planner "$WHOSCORED_PLANNER_IMAGE" \
  --arg signer "$WHOSCORED_SIGNER_IMAGE" '
    .schema_version == 2 and
    .status == "admitted-running-v1" and
    ([.images[] | select(.service == "airflow-scheduler") | .final_image]
      == [$planner]) and
    ([.images[] | select(.service == "whoscored_proxy_filter") | .final_image]
      == [$signer]) and
    ([.images[].service] | sort == [
      "airflow-scheduler",
      "flaresolverr",
      "flaresolverr_whoscored_paid",
      "whoscored_paid_gateway",
      "whoscored_proxy_filter"
    ]) and
    (.provider_policy.document_sha256
      | type == "string" and test("^[0-9a-f]{64}$"))
  ' "$running_admission_receipt" >/dev/null || fail "fresh running admission is invalid"

readonly logical_date="$($DATE -u --date='yesterday 10:00:00' '+%Y-%m-%dT10:00:00+00:00')"
readonly run_id="scheduled__${logical_date}"
readonly run_hash="$(printf '%s' "$run_id" | "$SHA256SUM" | cut -d' ' -f1)"
readonly planner_plan_container_path="/var/lib/whoscored/plans/${run_hash}.json"
readonly signer_plan_container_path="/authority/daily-plan.json"
readonly issuance_ledger_host_path="$WHOSCORED_ISSUANCE_LEDGER_HOST_DIR/issuance-ledger.json"

authority_stage="$(mktemp -d "$RUNTIME_DIRECTORY/authority.XXXXXXXX")"
planner_output_dir="$authority_stage/planner-output"
planner_output_path="$planner_output_dir/${run_hash}.json"
frozen_plan_host_path="$authority_stage/daily-plan.json"
require_root_private_directory "$authority_stage"
cleanup() {
  rm -f -- "$authority_stage/cohort.json" \
    "$authority_stage/provider-policy.json" \
    "$authority_stage/charter.json" \
    "$planner_output_path" \
    "$frozen_plan_host_path" \
    "$authority_stage/credentials/approval-hmac" \
    "$authority_stage/credentials/owner-hmac" \
    "$authority_stage/credentials/issuance-ledger-hmac"
  rmdir -- "$planner_output_dir" 2>/dev/null || true
  rmdir -- "$authority_stage/credentials" 2>/dev/null || true
  rmdir -- "$authority_stage" 2>/dev/null || true
}
trap cleanup EXIT
install -d -o 50000 -g 0 -m 0700 "$planner_output_dir"
for mapping in \
  "$WHOSCORED_COHORT_FILE:cohort.json" \
  "$WHOSCORED_PROVIDER_POLICY_FILE:provider-policy.json" \
  "$WHOSCORED_CHARTER_FILE:charter.json"
do
  source_path="${mapping%:*}"
  target_name="${mapping##*:}"
  install -o 50000 -g 0 -m 0400 "$source_path" "$authority_stage/$target_name"
done
readonly admitted_policy_sha256="$("$JQ" -er \
  '.provider_policy.document_sha256' "$running_admission_receipt")"
readonly staged_policy_document_sha256="$("$JQ" -er \
  '.document_sha256 | select(type == "string" and test("^[0-9a-f]{64}$"))' \
  "$authority_stage/provider-policy.json")"
test "$staged_policy_document_sha256" = "$admitted_policy_sha256" || \
  fail "staged provider policy differs from admission"
test "$("$SHA256SUM" "$WHOSCORED_PROVIDER_POLICY_FILE" | cut -d' ' -f1)" = \
  "$("$SHA256SUM" "$authority_stage/provider-policy.json" | cut -d' ' -f1)" || \
  fail "provider policy changed while it was staged"
install -d -o 50000 -g 0 -m 0700 "$authority_stage/credentials"
for name in approval-hmac owner-hmac issuance-ledger-hmac; do
  install -o 50000 -g 0 -m 0400 \
    "$CREDENTIALS_DIRECTORY/$name" "$authority_stage/credentials/$name"
done

docker_clean=(
  env -i
  HOME=/nonexistent
  LANG=C.UTF-8
  LC_ALL=C.UTF-8
  PATH=/usr/bin:/bin
  DOCKER_HOST=unix:///run/docker.sock
  "$DOCKER"
)

planner_deployed="$("${docker_clean[@]}" inspect --format '{{.Config.Image}}' airflow-scheduler)"
signer_deployed="$("${docker_clean[@]}" inspect --format '{{.Config.Image}}' whoscored_proxy_filter)"
test "$planner_deployed" = "$WHOSCORED_PLANNER_IMAGE" || fail "planner image differs from deployed scheduler"
test "$signer_deployed" = "$WHOSCORED_SIGNER_IMAGE" || fail "signer image differs from deployed paid filter"

common_mounts=(
  --mount "type=bind,src=$WHOSCORED_RELEASE_ROOT/dags,dst=/opt/airflow/dags,readonly"
  --mount "type=bind,src=$WHOSCORED_RELEASE_ROOT/scripts,dst=/opt/airflow/scripts,readonly"
  --mount "type=bind,src=$WHOSCORED_RELEASE_ROOT/scrapers,dst=/opt/airflow/scrapers,readonly"
  --mount "type=bind,src=$WHOSCORED_RELEASE_ROOT/configs/medallion,dst=/opt/airflow/configs/medallion,readonly"
)
container_hardening=(
  --read-only
  --user 50000:0
  --cap-drop ALL
  --security-opt no-new-privileges:true
  --security-opt apparmor=docker-default
  --security-opt seccomp=builtin
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=32m,uid=50000,gid=0,mode=0700
)

"${docker_clean[@]}" run --rm --pull never \
  --name "whoscored-daily-planner-${run_hash:0:12}" \
  --network "$PLANNER_NETWORK" \
  --env-file "$WHOSCORED_PLANNER_ENV_FILE" \
  "${container_hardening[@]}" \
  "${common_mounts[@]}" \
  --mount "type=bind,src=$authority_stage/cohort.json,dst=/authority/cohort.json,readonly" \
  --mount "type=bind,src=$planner_output_dir,dst=/var/lib/whoscored/plans" \
  "$WHOSCORED_PLANNER_IMAGE" \
  python /opt/airflow/scripts/whoscored_proxy_campaign.py \
    plan-daily-ingest \
    --cohort-file /authority/cohort.json \
    --max-scopes "$MAX_SCOPES" \
    --output "$planner_plan_container_path"
require_container_private_file "$planner_output_path"
install -o root -g root -m 0440 "$planner_output_path" "$frozen_plan_host_path"
require_frozen_container_file "$frozen_plan_host_path"
test "$("$SHA256SUM" "$planner_output_path" | cut -d' ' -f1)" = \
  "$("$SHA256SUM" "$frozen_plan_host_path" | cut -d' ' -f1)" || \
  fail "daily plan changed while it was frozen"
rm -f -- "$planner_output_path"
rmdir -- "$planner_output_dir"
"$JQ" -e --argjson max_scopes "$MAX_SCOPES" '
  .schema_version == 2 and
  .max_scopes == $max_scopes and
  (.scope_workloads | type == "array" and length >= 1 and length <= $max_scopes)
' "$frozen_plan_host_path" >/dev/null || fail "daily plan exceeds the exact scope bound"

"${docker_clean[@]}" run --rm --pull never \
  --name "whoscored-daily-signer-${run_hash:0:12}" \
  --network none \
  "${container_hardening[@]}" \
  "${common_mounts[@]}" \
  --mount "type=bind,src=$frozen_plan_host_path,dst=$signer_plan_container_path,readonly" \
  --mount "type=bind,src=$authority_stage/cohort.json,dst=/authority/cohort.json,readonly" \
  --mount "type=bind,src=$authority_stage/provider-policy.json,dst=/authority/provider-policy.json,readonly" \
  --mount "type=bind,src=$authority_stage/charter.json,dst=/authority/charter.json,readonly" \
  --mount "type=bind,src=$authority_stage/credentials,dst=/run/credentials,readonly" \
  --mount "type=bind,src=$WHOSCORED_PROXY_APPROVAL_HOST_DIR,dst=/authority/approvals" \
  --mount "type=bind,src=$WHOSCORED_SCHEDULED_PAID_POINTER_HOST_DIR,dst=/authority/pointers" \
  --mount "type=bind,src=$WHOSCORED_ISSUANCE_LEDGER_HOST_DIR,dst=/authority/ledger" \
  "$WHOSCORED_SIGNER_IMAGE" \
  python /opt/airflow/scripts/whoscored_proxy_campaign.py \
    issue-daily-ingest \
    --run-id "$run_id" \
    --plan-file "$signer_plan_container_path" \
    --cohort-file /authority/cohort.json \
    --max-scopes "$MAX_SCOPES" \
    --provider-policy /authority/provider-policy.json \
    --charter /authority/charter.json \
    --runtime-sha256 "$WHOSCORED_RUNTIME_SHA256" \
    --classifier-sha256 "$WHOSCORED_CLASSIFIER_SHA256" \
    --total-bytes "$TOTAL_BYTES" \
    --approval-root /authority/approvals \
    --pointer-root /authority/pointers \
    --issuance-ledger /authority/ledger/issuance-ledger.json \
    --secret-file /run/credentials/approval-hmac \
    --owner-secret-file /run/credentials/owner-hmac \
    --issuance-ledger-secret-file /run/credentials/issuance-ledger-hmac

test -f "$issuance_ledger_host_path" || fail "signer did not publish issuance ledger"
echo "WhoScored daily issuer completed for $run_id"
