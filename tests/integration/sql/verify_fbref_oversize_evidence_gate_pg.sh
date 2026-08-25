#!/usr/bin/env bash
# Disposable PostgreSQL regression for the FBref oversized-evidence gate.

set -Eeuo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "${script_dir}/../../.." && pwd)
gate_file="${repo_root}/docs/operations/sql/fbref_20260825_oversize_evidence_canary_gate.sql"
temp_dir=$(mktemp -d /tmp/fbref-oversize-gate-pg.XXXXXX)
container_name="fbref-oversize-gate-pg-${PPID}-${RANDOM}"
database_name="fbref_gate"
source_run_id="11111111-1111-1111-1111-111111111111"
diagnostic_run_id="22222222-2222-2222-2222-222222222222"
airflow_run_id="fbref-oversize-disposable-pg"
target_id="fbref:season_stats:1:2025-2026:standard"
orphan_target_id="fbref:season_stats:2:2025-2026:standard"
reviewed_gate="${temp_dir}/reviewed-gate.sql"
unreviewed_gate="${temp_dir}/unreviewed-gate.sql"

cleanup() {
    docker stop "${container_name}" >"${temp_dir}/docker-stop.log" 2>&1 || true
    rm -rf -- "${temp_dir}"
}
trap cleanup EXIT

docker run --rm --detach \
    --name "${container_name}" \
    --env POSTGRES_PASSWORD=fbref-gate-test \
    --env POSTGRES_DB="${database_name}" \
    postgres:16-alpine >"${temp_dir}/container-id"

ready=false
for _attempt in $(seq 1 30); do
    if docker exec "${container_name}" \
        pg_isready -U postgres -d "${database_name}" \
        >"${temp_dir}/pg-is-ready.log" 2>&1; then
        ready=true
        break
    fi
    sleep 1
done
if [[ "${ready}" != true ]]; then
    echo "disposable PostgreSQL did not become ready" >&2
    exit 1
fi

psql_cmd=(
    docker exec -i "${container_name}"
    psql -X --set=ON_ERROR_STOP=1 -U postgres -d "${database_name}"
)

"${psql_cmd[@]}" <<'SQL'
CREATE SCHEMA fbref_control;

CREATE TABLE fbref_control.crawl_run (
    run_id uuid PRIMARY KEY,
    run_type text NOT NULL,
    status text NOT NULL,
    request_limit bigint NOT NULL,
    byte_limit bigint NOT NULL,
    requests_used bigint NOT NULL,
    bytes_used bigint NOT NULL,
    budget_exceeded boolean NOT NULL,
    metadata jsonb NOT NULL,
    finished_at timestamptz
);

CREATE TABLE fbref_control.page_frontier (
    target_id text PRIMARY KEY,
    canonical_url text NOT NULL
);

CREATE TABLE fbref_control.run_target (
    run_id uuid NOT NULL REFERENCES fbref_control.crawl_run(run_id),
    target_id text NOT NULL REFERENCES fbref_control.page_frontier(target_id),
    logical_refresh_id uuid NOT NULL,
    ordinal integer NOT NULL,
    status text NOT NULL,
    PRIMARY KEY (run_id, target_id)
);

-- Deliberately mirrors the production-permitted shape: fetch_attempt has
-- individual run/frontier FKs, but no composite FK to run_target.
CREATE TABLE fbref_control.fetch_attempt (
    attempt_id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES fbref_control.crawl_run(run_id),
    target_id text NOT NULL REFERENCES fbref_control.page_frontier(target_id),
    logical_refresh_id uuid NOT NULL,
    attempt_number integer NOT NULL,
    status text NOT NULL,
    error_class text,
    error_message text,
    http_status integer,
    http_request_count bigint NOT NULL,
    http_status_history integer[] NOT NULL,
    decoded_bytes bigint NOT NULL,
    wire_bytes bigint NOT NULL,
    provider_billed_bytes bigint
);

CREATE TABLE fbref_control.publication_lock (
    owner_run_id uuid NOT NULL REFERENCES fbref_control.crawl_run(run_id),
    released_at timestamptz
);

INSERT INTO fbref_control.crawl_run VALUES
    (
        '11111111-1111-1111-1111-111111111111',
        'current', 'failed', 100, 52428800, 1, 5000, false,
        '{}'::jsonb, clock_timestamp()
    ),
    (
        '22222222-2222-2222-2222-222222222222',
        'current', 'succeeded', 100, 52428800, 1, 5000, false,
        jsonb_build_object(
            'airflow_run_id', 'fbref-oversize-disposable-pg',
            'dag_id', 'fbref_oversize_evidence_canary',
            'execution_mode', 'acceptance_nonpublishing',
            'publication_eligible', false,
            'acceptance_profile', true,
            'acceptance_scope', 'current',
            'shard_size', 25,
            'reviewed_source_run_id',
                '11111111-1111-1111-1111-111111111111',
            'reviewed_terminal_snapshot_sha256', repeat('0', 64)
        ),
        clock_timestamp()
    );

INSERT INTO fbref_control.page_frontier VALUES
    (
        'fbref:season_stats:1:2025-2026:standard',
        'https://fbref.com/en/comps/1/standard/Test-Stats'
    );

INSERT INTO fbref_control.run_target VALUES
    (
        '11111111-1111-1111-1111-111111111111',
        'fbref:season_stats:1:2025-2026:standard',
        'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', 0, 'failed'
    ),
    (
        '22222222-2222-2222-2222-222222222222',
        'fbref:season_stats:1:2025-2026:standard',
        'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', 0, 'succeeded'
    );

INSERT INTO fbref_control.fetch_attempt VALUES
    (
        'cccccccc-cccc-4ccc-8ccc-cccccccccccc',
        '11111111-1111-1111-1111-111111111111',
        'fbref:season_stats:1:2025-2026:standard',
        'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', 1, 'failed',
        'response_too_large',
        'FBref cumulative response bodies exceeded 4194304 bytes for https://fbref.com/en/comps/1/standard/Test-Stats',
        200, 1, ARRAY[200]::integer[], 0, 4194305, 4194305
    ),
    (
        'dddddddd-dddd-4ddd-8ddd-dddddddddddd',
        '22222222-2222-2222-2222-222222222222',
        'fbref:season_stats:1:2025-2026:standard',
        'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', 1, 'succeeded',
        NULL, NULL, 200, 1, ARRAY[200]::integer[], 1000, 1000, 1000
    );
SQL

snapshot_sha256=$("${psql_cmd[@]}" -Atc "
SELECT encode(sha256(convert_to(
    concat_ws(E'\\t', target.target_id, frontier.canonical_url,
              target.status, attempt.status, attempt.error_class,
              attempt.http_status::text, attempt.http_request_count::text,
              attempt.error_message) || E'\\n', 'UTF8')), 'hex')
FROM fbref_control.run_target AS target
JOIN fbref_control.page_frontier AS frontier
  ON frontier.target_id = target.target_id
JOIN fbref_control.fetch_attempt AS attempt
  ON attempt.run_id = target.run_id
 AND attempt.target_id = target.target_id
 AND attempt.logical_refresh_id = target.logical_refresh_id
WHERE target.run_id = '${source_run_id}'::uuid;")
if [[ ! "${snapshot_sha256}" =~ ^[0-9a-f]{64}$ ]]; then
    echo "fixture snapshot digest is not canonical SHA256" >&2
    exit 1
fi

"${psql_cmd[@]}" -c "UPDATE fbref_control.crawl_run
        SET metadata = jsonb_set(
            metadata,
            '{reviewed_terminal_snapshot_sha256}',
            to_jsonb('${snapshot_sha256}'::text)
        )
        WHERE run_id = '${diagnostic_run_id}'::uuid;"

sed \
    -e "s/^\\\\set fbref_oversize_baked_source_run_id 94838bac-786a-5d59-99e4-f6a2b3f7971e$/\\\\set fbref_oversize_baked_source_run_id ${source_run_id}/" \
    -e "s/^\\\\set fbref_oversize_baked_snapshot_sha256 b114e1139c50857b2985ead5ef2f72083660fc75cc9d1e9466874959a77bd543$/\\\\set fbref_oversize_baked_snapshot_sha256 ${snapshot_sha256}/" \
    "${gate_file}" >"${reviewed_gate}"
sed \
    -e 's/^\\set fbref_oversize_authority_state REVIEWED$/\\set fbref_oversize_authority_state UNREVIEWED/' \
    "${reviewed_gate}" >"${unreviewed_gate}"

run_gate() {
    local label=$1
    local sql_file=$2
    local output_file="${temp_dir}/${label}.log"
    local exit_code
    set +e
    "${psql_cmd[@]}" \
        --set=airflow_run_id="${airflow_run_id}" \
        <"${sql_file}" >"${output_file}" 2>&1
    exit_code=$?
    set -e
    printf '%s\n' "${exit_code}"
}

assert_pass() {
    local label=$1
    local exit_code
    exit_code=$(run_gate "${label}" "${reviewed_gate}")
    if [[ "${exit_code}" -ne 0 ]] ||
       ! grep -Fq 'PASS: exact FBref oversize diagnostic' \
           "${temp_dir}/${label}.log"; then
        sed -n '1,240p' "${temp_dir}/${label}.log" >&2
        echo "${label}: expected PASS" >&2
        exit 1
    fi
    echo "PASS [${label}]"
}

assert_no_go() {
    local label=$1
    local evidence=${2:-}
    local exit_code
    exit_code=$(run_gate "${label}" "${reviewed_gate}")
    if [[ "${exit_code}" -eq 0 ]] ||
       ! grep -Fq 'NO-GO' "${temp_dir}/${label}.log"; then
        sed -n '1,240p' "${temp_dir}/${label}.log" >&2
        echo "${label}: expected NO-GO/nonzero" >&2
        exit 1
    fi
    if [[ -n "${evidence}" ]] &&
       ! grep -Fq "${evidence}" "${temp_dir}/${label}.log"; then
        sed -n '1,240p' "${temp_dir}/${label}.log" >&2
        echo "${label}: missing bounded failure evidence" >&2
        exit 1
    fi
    echo "PASS [${label}=NO-GO]"
}

sentinel_code=$(run_gate "unreviewed-authority" "${unreviewed_gate}")
if [[ "${sentinel_code}" -eq 0 ]] ||
   ! grep -Fq 'authority is unreviewed' \
       "${temp_dir}/unreviewed-authority.log"; then
    sed -n '1,120p' "${temp_dir}/unreviewed-authority.log" >&2
    echo "unreviewed-authority: expected preflight abort" >&2
    exit 1
fi
echo 'PASS [unreviewed-authority=ABORT]'

assert_pass "reviewed-pass"

"${psql_cmd[@]}" -c "
UPDATE fbref_control.crawl_run
SET requests_used = 2
WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "run-accounting-mismatch"
"${psql_cmd[@]}" -c "
UPDATE fbref_control.crawl_run
SET requests_used = 1
WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "
UPDATE fbref_control.crawl_run
SET metadata = jsonb_set(
    metadata,
    '{reviewed_source_run_id}',
    to_jsonb('99999999-9999-4999-8999-999999999999'::text)
)
WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "provenance-mismatch"
"${psql_cmd[@]}" -c "
UPDATE fbref_control.crawl_run
SET metadata = jsonb_set(
    metadata,
    '{reviewed_source_run_id}',
    to_jsonb('${source_run_id}'::text)
)
WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "
UPDATE fbref_control.fetch_attempt
SET error_message = error_message || ' changed'
WHERE run_id = '${source_run_id}'::uuid;"
assert_no_go "source-digest-mismatch"
"${psql_cmd[@]}" -c "
UPDATE fbref_control.fetch_attempt
SET error_message = regexp_replace(error_message, ' changed$', '')
WHERE run_id = '${source_run_id}'::uuid;"

"${psql_cmd[@]}" -c "
INSERT INTO fbref_control.page_frontier VALUES (
    '${orphan_target_id}',
    'https://fbref.com/en/comps/2/standard/Orphan-Stats'
);
INSERT INTO fbref_control.fetch_attempt VALUES (
    'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee',
    '${diagnostic_run_id}'::uuid,
    '${orphan_target_id}',
    'ffffffff-ffff-4fff-8fff-ffffffffffff',
    1, 'succeeded', NULL, NULL, 200, 1, ARRAY[200]::integer[],
    1000, 1000, 1000
);
UPDATE fbref_control.crawl_run
SET requests_used = 2
WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "orphan-extra-attempt" "${orphan_target_id}"
"${psql_cmd[@]}" -c "
DELETE FROM fbref_control.fetch_attempt
WHERE attempt_id = 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee'::uuid;
DELETE FROM fbref_control.page_frontier
WHERE target_id = '${orphan_target_id}';
UPDATE fbref_control.crawl_run
SET requests_used = 1
WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "
UPDATE fbref_control.fetch_attempt
SET http_request_count = 2
WHERE run_id = '${diagnostic_run_id}'::uuid;
UPDATE fbref_control.crawl_run
SET requests_used = 2
WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "extra-request"
"${psql_cmd[@]}" -c "
UPDATE fbref_control.fetch_attempt
SET http_request_count = 1
WHERE run_id = '${diagnostic_run_id}'::uuid;
UPDATE fbref_control.crawl_run
SET requests_used = 1
WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "
UPDATE fbref_control.fetch_attempt
SET logical_refresh_id = 'ffffffff-ffff-4fff-8fff-ffffffffffff'::uuid
WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "foreign-logical-refresh"
"${psql_cmd[@]}" -c "
UPDATE fbref_control.fetch_attempt
SET logical_refresh_id = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'::uuid
WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "
UPDATE fbref_control.crawl_run
SET status = 'failed'
WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "failed-run"

echo 'Disposable PostgreSQL gate matrix: PASS'
