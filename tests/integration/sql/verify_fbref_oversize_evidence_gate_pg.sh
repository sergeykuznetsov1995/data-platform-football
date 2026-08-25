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
orphan_target_id="fbref:season_stats:999:2025-2026:standard"
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
    requests_reserved bigint NOT NULL,
    bytes_reserved bigint NOT NULL,
    requests_used bigint NOT NULL,
    bytes_used bigint NOT NULL,
    budget_exceeded boolean NOT NULL,
    metadata jsonb NOT NULL,
    finished_at timestamptz
);

CREATE TABLE fbref_control.season_registry (
    source text NOT NULL,
    competition_id text NOT NULL,
    season_id text NOT NULL,
    present boolean NOT NULL,
    lifecycle_state text NOT NULL,
    is_current boolean NOT NULL,
    PRIMARY KEY (source, competition_id, season_id)
);

CREATE TABLE fbref_control.page_frontier (
    target_id text PRIMARY KEY,
    canonical_url text NOT NULL,
    source text NOT NULL,
    page_kind text NOT NULL,
    refresh_policy text NOT NULL,
    state text NOT NULL,
    next_fetch_at timestamptz,
    last_error_class text,
    last_error_message text,
    last_http_status integer,
    last_fetched_at timestamptz
);

CREATE TABLE fbref_control.run_target (
    run_id uuid NOT NULL REFERENCES fbref_control.crawl_run(run_id),
    target_id text NOT NULL REFERENCES fbref_control.page_frontier(target_id),
    logical_refresh_id uuid NOT NULL,
    ordinal integer NOT NULL,
    status text NOT NULL,
    PRIMARY KEY (run_id, target_id)
);

-- Production intentionally has no composite FK from attempt to run_target.
CREATE TABLE fbref_control.fetch_attempt (
    attempt_id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES fbref_control.crawl_run(run_id),
    target_id text NOT NULL REFERENCES fbref_control.page_frontier(target_id),
    logical_refresh_id uuid NOT NULL,
    attempt_number integer NOT NULL,
    reservation_id uuid,
    status text NOT NULL,
    error_class text,
    error_message text,
    http_status integer,
    http_request_count bigint NOT NULL,
    http_status_history integer[] NOT NULL,
    content_hash text,
    raw_manifest_key text,
    decoded_bytes bigint NOT NULL,
    compressed_bytes bigint NOT NULL,
    wire_bytes bigint NOT NULL,
    provider_billed_bytes bigint,
    transport_version text,
    session_version text
);

CREATE TABLE fbref_control.publication_lock (
    owner_run_id uuid NOT NULL REFERENCES fbref_control.crawl_run(run_id),
    released_at timestamptz
);

CREATE TABLE fbref_control.budget_reservation (
    reservation_id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES fbref_control.crawl_run(run_id),
    logical_refresh_id uuid NOT NULL,
    requests_reserved bigint NOT NULL,
    bytes_reserved bigint NOT NULL,
    status text NOT NULL,
    requests_used bigint NOT NULL,
    bytes_used bigint NOT NULL
);

CREATE TABLE fbref_control.clearance_session (
    session_id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES fbref_control.crawl_run(run_id),
    status text NOT NULL,
    browser_bootstrap_attempts bigint NOT NULL,
    browser_bootstrap_requests bigint NOT NULL,
    browser_document_bytes bigint NOT NULL,
    browser_asset_bytes bigint NOT NULL,
    browser_unobserved_bytes bigint NOT NULL,
    http_requests bigint NOT NULL,
    http_wire_bytes bigint NOT NULL,
    decoded_html_bytes bigint NOT NULL,
    compressed_raw_bytes bigint NOT NULL,
    provider_billed_bytes bigint
);

CREATE TABLE fbref_control.clearance_session_page_accounting (
    reservation_id uuid PRIMARY KEY
        REFERENCES fbref_control.budget_reservation(reservation_id),
    session_id uuid NOT NULL
        REFERENCES fbref_control.clearance_session(session_id),
    run_id uuid NOT NULL REFERENCES fbref_control.crawl_run(run_id),
    attempt_id uuid NOT NULL UNIQUE
        REFERENCES fbref_control.fetch_attempt(attempt_id),
    requests_used bigint NOT NULL,
    browser_bootstrap_attempts bigint NOT NULL,
    browser_bootstrap_requests bigint NOT NULL,
    browser_document_bytes bigint NOT NULL,
    browser_asset_bytes bigint NOT NULL,
    browser_unobserved_bytes bigint NOT NULL,
    http_requests bigint NOT NULL,
    decoded_html_bytes bigint NOT NULL,
    compressed_raw_bytes bigint NOT NULL,
    http_wire_bytes bigint NOT NULL,
    provider_billed_bytes bigint NOT NULL,
    evidence_sha256 text NOT NULL
);

CREATE TABLE fbref_control.clearance_session_tail_reservation (
    session_id uuid PRIMARY KEY
        REFERENCES fbref_control.clearance_session(session_id),
    run_id uuid NOT NULL REFERENCES fbref_control.crawl_run(run_id),
    reservation_id uuid NOT NULL UNIQUE
        REFERENCES fbref_control.budget_reservation(reservation_id),
    baseline_provider_bytes bigint NOT NULL,
    bytes_reserved bigint NOT NULL,
    status text NOT NULL,
    page_provider_bytes bigint,
    authoritative_provider_bytes bigint,
    tail_provider_bytes bigint,
    settlement_sha256 text
);

INSERT INTO fbref_control.crawl_run VALUES
    (
        '11111111-1111-1111-1111-111111111111',
        'current', 'failed', 100, 52428800, 0, 0, 4, 16777220, false,
        '{}'::jsonb, clock_timestamp()
    ),
    (
        '22222222-2222-2222-2222-222222222222',
        'current', 'succeeded', 100, 52428800, 0, 0, 22, 5100, false,
        jsonb_build_object(
            'airflow_run_id', 'fbref-oversize-disposable-pg',
            'dag_id', 'fbref_oversize_evidence_canary',
            'execution_mode', 'acceptance_nonpublishing',
            'publication_eligible', false,
            'acceptance_profile', true,
            'acceptance_scope', 'current',
            'persistent_http_session', true,
            'browser_request_limit', 20,
            'browser_solve_limit', 1,
            'provider_dag_id', 'dag_accept_fbref_bronze',
            'provider_task_id', 'oversize_evidence_fetch',
            'provider_scope', 'fbref-oversize-disposable-pg',
            'provider_run_id',
                '22222222-2222-2222-2222-222222222222',
            'provider_byte_limit', 39321600,
            'shard_size', 25,
            'request_reservation_bytes', 9437184,
            'reviewed_source_run_id',
                '11111111-1111-1111-1111-111111111111',
            'reviewed_terminal_snapshot_sha256', repeat('0', 64),
            'reviewed_diagnostic_target_ids', jsonb_build_array(
                'fbref:season_stats:569:2025-2026:playingtime',
                'fbref:season_stats:569:2025-2026:standard'
            )
        ),
        clock_timestamp()
    );

INSERT INTO fbref_control.season_registry VALUES
    ('fbref', '6', '2022', true, 'present', false),
    ('fbref', '678', '2021', true, 'present', false),
    ('fbref', '569', '2025-2026', true, 'present', true);

INSERT INTO fbref_control.page_frontier VALUES
    (
        'fbref:season_stats:6:2022:playingtime',
        'https://fbref.com/en/comps/6/2022/playingtime/2022-WCQ----UEFA-M-Stats',
        'fbref', 'season_stats', 'daily', 'quarantined', NULL,
        'ScopeQuarantined', 'noncurrent_season', 200, NULL
    ),
    (
        'fbref:season_stats:569:2025-2026:playingtime',
        'https://fbref.com/en/comps/569/playingtime/Copa-del-Rey-Stats',
        'fbref', 'season_stats', 'daily', 'fetched', NULL,
        NULL, NULL, 200, clock_timestamp()
    ),
    (
        'fbref:season_stats:569:2025-2026:standard',
        'https://fbref.com/en/comps/569/stats/Copa-del-Rey-Stats',
        'fbref', 'season_stats', 'daily', 'fetched', NULL,
        NULL, NULL, 200, clock_timestamp()
    ),
    (
        'fbref:season_stats:678:2021:playingtime',
        'https://fbref.com/en/comps/678/2021/playingtime/2021-UEFA-Euro-qualification-Stats',
        'fbref', 'season_stats', 'daily', 'quarantined', NULL,
        'ScopeQuarantined', 'noncurrent_season', 200, NULL
    );

INSERT INTO fbref_control.run_target VALUES
    ('11111111-1111-1111-1111-111111111111',
     'fbref:season_stats:6:2022:playingtime',
     '10000000-0000-4000-8000-000000000001', 0, 'failed'),
    ('11111111-1111-1111-1111-111111111111',
     'fbref:season_stats:569:2025-2026:playingtime',
     '10000000-0000-4000-8000-000000000002', 1, 'failed'),
    ('11111111-1111-1111-1111-111111111111',
     'fbref:season_stats:569:2025-2026:standard',
     '10000000-0000-4000-8000-000000000003', 2, 'failed'),
    ('11111111-1111-1111-1111-111111111111',
     'fbref:season_stats:678:2021:playingtime',
     '10000000-0000-4000-8000-000000000004', 3, 'failed'),
    ('22222222-2222-2222-2222-222222222222',
     'fbref:season_stats:569:2025-2026:playingtime',
     '20000000-0000-4000-8000-000000000001', 0, 'succeeded'),
    ('22222222-2222-2222-2222-222222222222',
     'fbref:season_stats:569:2025-2026:standard',
     '20000000-0000-4000-8000-000000000002', 1, 'succeeded');

INSERT INTO fbref_control.fetch_attempt VALUES
    (
        '30000000-0000-4000-8000-000000000001',
        '11111111-1111-1111-1111-111111111111',
        'fbref:season_stats:6:2022:playingtime',
        '10000000-0000-4000-8000-000000000001', 1, NULL, 'failed',
        'response_too_large',
        'FBref cumulative response bodies exceeded 4194304 bytes for https://fbref.com/en/comps/6/2022/playingtime/2022-WCQ----UEFA-M-Stats',
        200, 1, ARRAY[200]::integer[], NULL, NULL,
        0, 0, 4194305, 4194305, NULL, NULL
    ),
    (
        '30000000-0000-4000-8000-000000000002',
        '11111111-1111-1111-1111-111111111111',
        'fbref:season_stats:569:2025-2026:playingtime',
        '10000000-0000-4000-8000-000000000002', 1, NULL, 'failed',
        'response_too_large',
        'FBref cumulative response bodies exceeded 4194304 bytes for https://fbref.com/en/comps/569/playingtime/Copa-del-Rey-Stats',
        200, 1, ARRAY[200]::integer[], NULL, NULL,
        0, 0, 4194305, 4194305, NULL, NULL
    ),
    (
        '30000000-0000-4000-8000-000000000003',
        '11111111-1111-1111-1111-111111111111',
        'fbref:season_stats:569:2025-2026:standard',
        '10000000-0000-4000-8000-000000000003', 1, NULL, 'failed',
        'response_too_large',
        'FBref cumulative response bodies exceeded 4194304 bytes for https://fbref.com/en/comps/569/stats/Copa-del-Rey-Stats',
        200, 1, ARRAY[200]::integer[], NULL, NULL,
        0, 0, 4194305, 4194305, NULL, NULL
    ),
    (
        '30000000-0000-4000-8000-000000000004',
        '11111111-1111-1111-1111-111111111111',
        'fbref:season_stats:678:2021:playingtime',
        '10000000-0000-4000-8000-000000000004', 1, NULL, 'failed',
        'response_too_large',
        'FBref cumulative response bodies exceeded 4194304 bytes for https://fbref.com/en/comps/678/2021/playingtime/2021-UEFA-Euro-qualification-Stats',
        200, 1, ARRAY[200]::integer[], NULL, NULL,
        0, 0, 4194305, 4194305, NULL, NULL
    ),
    (
        '40000000-0000-4000-8000-000000000001',
        '22222222-2222-2222-2222-222222222222',
        'fbref:season_stats:569:2025-2026:playingtime',
        '20000000-0000-4000-8000-000000000001', 1,
        '50000000-0000-4000-8000-000000000001', 'succeeded',
        NULL, NULL, 200, 1, ARRAY[200]::integer[],
        repeat('a', 64),
        'manifests/fetches/20000000-0000-4000-8000-000000000001.json',
        5000000, 1200, 3000, 3000,
        'fbref-camoufox-metered-warm-http-v10',
        '60000000-0000-4000-8000-000000000001'
    ),
    (
        '40000000-0000-4000-8000-000000000002',
        '22222222-2222-2222-2222-222222222222',
        'fbref:season_stats:569:2025-2026:standard',
        '20000000-0000-4000-8000-000000000002', 1,
        '50000000-0000-4000-8000-000000000002', 'succeeded',
        NULL, NULL, 200, 1, ARRAY[200]::integer[],
        repeat('b', 64),
        'manifests/fetches/20000000-0000-4000-8000-000000000002.json',
        6000000, 1500, 2000, 2000,
        'fbref-camoufox-metered-warm-http-v10',
        '60000000-0000-4000-8000-000000000001'
    );

INSERT INTO fbref_control.budget_reservation VALUES
    ('50000000-0000-4000-8000-000000000001',
     '22222222-2222-2222-2222-222222222222',
     '20000000-0000-4000-8000-000000000001', 22, 13631488,
     'settled', 21, 3000),
    ('50000000-0000-4000-8000-000000000002',
     '22222222-2222-2222-2222-222222222222',
     '20000000-0000-4000-8000-000000000002', 2, 9437184,
     'settled', 1, 2000),
    ('50000000-0000-4000-8000-000000000003',
     '22222222-2222-2222-2222-222222222222',
     '60000000-0000-4000-8000-000000000001', 0, 9437184,
     'settled', 0, 100);

INSERT INTO fbref_control.clearance_session VALUES
    ('60000000-0000-4000-8000-000000000001',
     '22222222-2222-2222-2222-222222222222', 'closed',
     1, 20, 100, 200, 300, 2, 5000, 11000000, 2700, 5100);

INSERT INTO fbref_control.clearance_session_page_accounting VALUES
    ('50000000-0000-4000-8000-000000000001',
     '60000000-0000-4000-8000-000000000001',
     '22222222-2222-2222-2222-222222222222',
     '40000000-0000-4000-8000-000000000001',
     21, 1, 20, 100, 200, 300, 1, 5000000, 1200, 3000, 3000,
     '74fea6cd98dd9d5c21e4dcf1fd258b407447dc9077bc19aeb21338ae6e9d27d2'),
    ('50000000-0000-4000-8000-000000000002',
     '60000000-0000-4000-8000-000000000001',
     '22222222-2222-2222-2222-222222222222',
     '40000000-0000-4000-8000-000000000002',
     1, 0, 0, 0, 0, 0, 1, 6000000, 1500, 2000, 2000,
     '8ae7a014dea1caacd1b58dc75be6fb498b40bedac6566b5809141c004dc105c1');

INSERT INTO fbref_control.clearance_session_tail_reservation VALUES
    ('60000000-0000-4000-8000-000000000001',
     '22222222-2222-2222-2222-222222222222',
     '50000000-0000-4000-8000-000000000003', 0, 9437184,
     'settled', 5000, 5100, 100,
     '046ba804dde161fbee1993eebd5935d3aff111cce4bfb644f21ad7630572080f');
SQL

snapshot_sha256=$("${psql_cmd[@]}" -Atc "
SELECT encode(sha256(convert_to(string_agg(
    concat_ws(E'\\t', target.target_id, frontier.canonical_url,
              target.status, attempt.status, attempt.error_class,
              attempt.http_status::text, attempt.http_request_count::text,
              attempt.error_message), E'\\n' ORDER BY target.target_id)
              || E'\\n', 'UTF8')), 'hex')
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
        sed -n '1,260p' "${temp_dir}/${label}.log" >&2
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
        sed -n '1,260p' "${temp_dir}/${label}.log" >&2
        echo "${label}: expected NO-GO/nonzero" >&2
        exit 1
    fi
    if [[ -n "${evidence}" ]] &&
       ! grep -Fq "${evidence}" "${temp_dir}/${label}.log"; then
        sed -n '1,260p' "${temp_dir}/${label}.log" >&2
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
assert_pass "realistic-browser-bootstrap"

"${psql_cmd[@]}" -c "UPDATE fbref_control.crawl_run
SET metadata = jsonb_set(metadata, '{browser_request_limit}', '21'::jsonb)
WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "browser-profile-metadata-mismatch"
"${psql_cmd[@]}" -c "UPDATE fbref_control.crawl_run
SET metadata = jsonb_set(metadata, '{browser_request_limit}', '20'::jsonb)
WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.crawl_run
SET metadata = jsonb_set(metadata, '{provider_byte_limit}', '39321601'::jsonb)
WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "provider-profile-metadata-mismatch"
"${psql_cmd[@]}" -c "UPDATE fbref_control.crawl_run
SET metadata = jsonb_set(metadata, '{provider_byte_limit}', '39321600'::jsonb)
WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.clearance_session
SET provider_billed_bytes = NULL
WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "null-session-provider-bytes"
"${psql_cmd[@]}" -c "UPDATE fbref_control.clearance_session
SET provider_billed_bytes = 5100
WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.clearance_session_tail_reservation
SET page_provider_bytes = 4900, authoritative_provider_bytes = 5000
WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "contradictory-tail-receipt"
"${psql_cmd[@]}" -c "UPDATE fbref_control.clearance_session_tail_reservation
SET page_provider_bytes = 5000, authoritative_provider_bytes = 5100
WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.clearance_session
SET decoded_html_bytes = 11000001
WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "session-byte-accounting-mismatch"
"${psql_cmd[@]}" -c "UPDATE fbref_control.clearance_session
SET decoded_html_bytes = 11000000
WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.clearance_session_page_accounting
SET evidence_sha256 = repeat('b', 64)
WHERE reservation_id = '50000000-0000-4000-8000-000000000001'::uuid;"
assert_no_go "page-evidence-digest-mismatch"
"${psql_cmd[@]}" -c "UPDATE fbref_control.clearance_session_page_accounting
SET evidence_sha256 =
    '74fea6cd98dd9d5c21e4dcf1fd258b407447dc9077bc19aeb21338ae6e9d27d2'
WHERE reservation_id = '50000000-0000-4000-8000-000000000001'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.clearance_session_tail_reservation
SET settlement_sha256 = repeat('b', 64)
WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "tail-evidence-digest-mismatch"
"${psql_cmd[@]}" -c "UPDATE fbref_control.clearance_session_tail_reservation
SET settlement_sha256 =
    '046ba804dde161fbee1993eebd5935d3aff111cce4bfb644f21ad7630572080f'
WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.crawl_run
SET requests_reserved = 1 WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "outstanding-run-reservation"
"${psql_cmd[@]}" -c "UPDATE fbref_control.crawl_run
SET requests_reserved = 0 WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET reservation_id = '50000000-0000-4000-8000-000000000003'::uuid
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"
assert_no_go "attempt-reservation-mismatch"
"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET reservation_id = '50000000-0000-4000-8000-000000000001'::uuid
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.budget_reservation
SET logical_refresh_id = '90000000-0000-4000-8000-000000000001'::uuid
WHERE reservation_id = '50000000-0000-4000-8000-000000000001'::uuid;"
assert_no_go "reservation-logical-refresh-mismatch"
"${psql_cmd[@]}" -c "UPDATE fbref_control.budget_reservation
SET logical_refresh_id = '20000000-0000-4000-8000-000000000001'::uuid
WHERE reservation_id = '50000000-0000-4000-8000-000000000001'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.budget_reservation
SET bytes_reserved = 9437185
WHERE reservation_id = '50000000-0000-4000-8000-000000000002'::uuid;"
assert_no_go "target-reservation-size-mismatch"
"${psql_cmd[@]}" -c "UPDATE fbref_control.budget_reservation
SET bytes_reserved = 9437184
WHERE reservation_id = '50000000-0000-4000-8000-000000000002'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.crawl_run
SET requests_used = 23 WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "run-accounting-mismatch"
"${psql_cmd[@]}" -c "UPDATE fbref_control.crawl_run
SET requests_used = 22 WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.clearance_session
SET browser_bootstrap_requests = 19
WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "browser-accounting-mismatch"
"${psql_cmd[@]}" -c "UPDATE fbref_control.clearance_session
SET browser_bootstrap_requests = 20
WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" <<SQL
UPDATE fbref_control.clearance_session_page_accounting
SET browser_document_bytes = 4194305,
    browser_asset_bytes = 0,
    browser_unobserved_bytes = 0
WHERE reservation_id = '50000000-0000-4000-8000-000000000001'::uuid;
UPDATE fbref_control.clearance_session
SET browser_document_bytes = 4194305,
    browser_asset_bytes = 0,
    browser_unobserved_bytes = 0
WHERE run_id = '${diagnostic_run_id}'::uuid;
UPDATE fbref_control.clearance_session_page_accounting AS page
SET evidence_sha256 = encode(sha256(convert_to(concat(
    '{"attempt_id":"', page.attempt_id::text,
    '","browser_asset_bytes":', page.browser_asset_bytes::text,
    ',"browser_bootstrap_attempts":',
        page.browser_bootstrap_attempts::text,
    ',"browser_bootstrap_requests":',
        page.browser_bootstrap_requests::text,
    ',"browser_document_bytes":', page.browser_document_bytes::text,
    ',"browser_unobserved_bytes":', page.browser_unobserved_bytes::text,
    ',"compressed_raw_bytes":', page.compressed_raw_bytes::text,
    ',"decoded_html_bytes":', page.decoded_html_bytes::text,
    ',"http_requests":', page.http_requests::text,
    ',"http_wire_bytes":', page.http_wire_bytes::text,
    ',"provider_billed_bytes":', page.provider_billed_bytes::text,
    ',"requests_used":', page.requests_used::text,
    ',"reservation_id":"', page.reservation_id::text,
    '","session_id":"', page.session_id::text, '"}'
), 'UTF8')), 'hex')
WHERE reservation_id = '50000000-0000-4000-8000-000000000001'::uuid;
SQL
assert_no_go "browser-byte-cap-overrun"
"${psql_cmd[@]}" -c "UPDATE fbref_control.clearance_session_page_accounting
SET browser_document_bytes = 100,
    browser_asset_bytes = 200,
    browser_unobserved_bytes = 300,
    evidence_sha256 =
        '74fea6cd98dd9d5c21e4dcf1fd258b407447dc9077bc19aeb21338ae6e9d27d2'
WHERE reservation_id = '50000000-0000-4000-8000-000000000001'::uuid;
UPDATE fbref_control.clearance_session
SET browser_document_bytes = 100,
    browser_asset_bytes = 200,
    browser_unobserved_bytes = 300
WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET content_hash = 'not-a-sha256'
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"
assert_no_go "malformed-raw-content-hash"
"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET content_hash = repeat('a', 64)
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET raw_manifest_key = 'manifests/fetches/foreign.json'
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"
assert_no_go "foreign-raw-manifest-key"
"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET raw_manifest_key =
    'manifests/fetches/20000000-0000-4000-8000-000000000001.json'
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET transport_version = NULL
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"
assert_no_go "null-attempt-transport"
"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET transport_version = 'fbref-camoufox-metered-warm-http-v10'
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET transport_version = 'fbref-camoufox-metered-warm-http-v9'
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"
assert_no_go "foreign-fetcher-version"
"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET transport_version = 'fbref-camoufox-metered-warm-http-v10'
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET session_version = '70000000-0000-4000-8000-000000000001'
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"
assert_no_go "foreign-attempt-session"
"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET session_version = '60000000-0000-4000-8000-000000000001'
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"

"${psql_cmd[@]}" -c "
UPDATE fbref_control.crawl_run
SET requests_used = 23 WHERE run_id = '${diagnostic_run_id}'::uuid;
UPDATE fbref_control.budget_reservation
SET requests_used = 22
WHERE reservation_id = '50000000-0000-4000-8000-000000000001'::uuid;
UPDATE fbref_control.clearance_session_page_accounting
SET requests_used = 22, browser_bootstrap_requests = 21
WHERE reservation_id = '50000000-0000-4000-8000-000000000001'::uuid;
UPDATE fbref_control.clearance_session
SET browser_bootstrap_requests = 21
WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "browser-reservation-overrun"
"${psql_cmd[@]}" -c "
UPDATE fbref_control.crawl_run
SET requests_used = 22 WHERE run_id = '${diagnostic_run_id}'::uuid;
UPDATE fbref_control.budget_reservation
SET requests_used = 21
WHERE reservation_id = '50000000-0000-4000-8000-000000000001'::uuid;
UPDATE fbref_control.clearance_session_page_accounting
SET requests_used = 21, browser_bootstrap_requests = 20
WHERE reservation_id = '50000000-0000-4000-8000-000000000001'::uuid;
UPDATE fbref_control.clearance_session
SET browser_bootstrap_requests = 20
WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET decoded_bytes = 4999999
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"
assert_no_go "raw-decoded-loss-mismatch"
"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET decoded_bytes = 5000000
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.crawl_run
SET metadata = jsonb_set(
    metadata, '{reviewed_source_run_id}',
    to_jsonb('99999999-9999-4999-8999-999999999999'::text)
) WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "provenance-mismatch"
"${psql_cmd[@]}" -c "UPDATE fbref_control.crawl_run
SET metadata = jsonb_set(
    metadata, '{reviewed_source_run_id}', to_jsonb('${source_run_id}'::text)
) WHERE run_id = '${diagnostic_run_id}'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET error_message = error_message || ' changed'
WHERE run_id = '${source_run_id}'::uuid
  AND target_id = 'fbref:season_stats:6:2022:playingtime';"
assert_no_go "source-digest-mismatch"
"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET error_message = regexp_replace(error_message, ' changed$', '')
WHERE run_id = '${source_run_id}'::uuid
  AND target_id = 'fbref:season_stats:6:2022:playingtime';"

"${psql_cmd[@]}" -c "UPDATE fbref_control.season_registry
SET is_current = true
WHERE source = 'fbref' AND competition_id = '6' AND season_id = '2022';"
assert_no_go "demoted-scope-mismatch"
"${psql_cmd[@]}" -c "UPDATE fbref_control.season_registry
SET is_current = false
WHERE source = 'fbref' AND competition_id = '6' AND season_id = '2022';"

"${psql_cmd[@]}" -c "UPDATE fbref_control.season_registry
SET is_current = false
WHERE source = 'fbref' AND competition_id = '569'
  AND season_id = '2025-2026';"
assert_no_go "genuine-current-mismatch"
"${psql_cmd[@]}" -c "UPDATE fbref_control.season_registry
SET is_current = true
WHERE source = 'fbref' AND competition_id = '569'
  AND season_id = '2025-2026';"

"${psql_cmd[@]}" -c "
INSERT INTO fbref_control.page_frontier VALUES (
    '${orphan_target_id}',
    'https://fbref.com/en/comps/999/stats/Orphan-Stats',
    'fbref', 'season_stats', 'daily', 'fetched', NULL,
    NULL, NULL, 200, clock_timestamp()
);
INSERT INTO fbref_control.fetch_attempt VALUES (
    '70000000-0000-4000-8000-000000000001',
    '${diagnostic_run_id}'::uuid, '${orphan_target_id}',
    '70000000-0000-4000-8000-000000000002',
    1, NULL, 'succeeded', NULL, NULL, 200, 1, ARRAY[200]::integer[],
    'orphan-content', 'raw/manifests/orphan.json',
    1000, 500, 1000, 1000,
    'fbref-camoufox-metered-warm-http-v10',
    '60000000-0000-4000-8000-000000000001'
);"
assert_no_go "orphan-extra-attempt" "${orphan_target_id}"
"${psql_cmd[@]}" -c "
DELETE FROM fbref_control.fetch_attempt
WHERE attempt_id = '70000000-0000-4000-8000-000000000001'::uuid;
DELETE FROM fbref_control.page_frontier
WHERE target_id = '${orphan_target_id}';"

"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET http_request_count = 2, http_status_history = ARRAY[200,200]::integer[]
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"
assert_no_go "extra-target-http-request"
"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET http_request_count = 1, http_status_history = ARRAY[200]::integer[]
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET logical_refresh_id = '80000000-0000-4000-8000-000000000001'::uuid
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"
assert_no_go "foreign-logical-refresh"
"${psql_cmd[@]}" -c "UPDATE fbref_control.fetch_attempt
SET logical_refresh_id = '20000000-0000-4000-8000-000000000001'::uuid
WHERE attempt_id = '40000000-0000-4000-8000-000000000001'::uuid;"

"${psql_cmd[@]}" -c "UPDATE fbref_control.crawl_run
SET status = 'failed' WHERE run_id = '${diagnostic_run_id}'::uuid;"
assert_no_go "failed-run"

echo 'Disposable PostgreSQL gate matrix: PASS'
