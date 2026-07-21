# WhoScored paid-proxy measurement campaign

This runbook covers the manual measurement canary only. Its legacy signed
envelope is `1_000_000_000` decimal provider-billed bytes and it is valid only
for `dag_canary_whoscored_proxy`; the provider order also has a 1.00 decimal-GB
quota. Neither value is executable authority for the whole amount. This release
enforces the stricter exact `300000000` decimal-byte lifetime ceiling across the
initialized provider-order state and does not authorize a full paid crawl. That
durable total includes spent bytes and active escrow from every approval and
campaign, and does not reset at UTC midnight, process restart, or approval
replacement.

The two code-owned invoice-boundary and application-gateway sentinels enable
only the exact canary contract; they are not environment switches or operator
authorization. A canary is executable only after production admission accepts
both an exact schema-v1 deployment attestation with `status=ready-v1` and a
fresh canonical provider quota receipt. The receipt must prove active
PROXYS.IO order `38950`, plan `Bronze`, and exact quota/remaining values of
`1.00` decimal GB, with its protected screenshot identity and observation time.
Admission also binds the five protected service images, rendered topology,
mounts and runtime hardening. The DAG then independently requires the exact
signed approval, an active gateway-authenticated campaign snapshot, and a real
gateway-owned production alert receipt. A missing, stale or mismatched proof
fails closed before paid I/O.

`WHOSCORED_FULL_PAID_CRAWL_AVAILABLE=False` remains a separate code-owned
sentinel. Normal ingest and backfill paid traffic are unavailable in this
release even if the measurement canary succeeds; neither DagRun configuration
nor environment variables can enable them.

The ceiling is authority, not a spending target. Raw-cache hits and successful
direct requests cost zero, and the workflow never manufactures traffic merely
to consume the `300000000`-byte release allowance.

## Safety properties

- The DAG is manual, paused on creation, and limited to one active DagRun.
- All source work uses the existing `whoscored_direct_pool`, which must have
  exactly two slots. The source-sensitive canary execution holds both slots
  continuously across discovery, cohort freeze and capture.
- Transport remains raw-cache first and direct first. Paid fallback is admitted
  only after an authoritative Cloudflare block and a fresh direct recheck.
- DagRun configuration contains only `transport_policy`, approval ID and
  approval SHA-256. Byte caps, hosts, paths, limits and allocations come only
  from the signed document. The scheduler performs structural and identity
  checks; the isolated gateway and filter perform HMAC verification.
- The approval permits the exact root path plus the bounded production
  families `/Matches`, `/Players`, `/Regions`, `/stagestatfeed`,
  `/statisticsfeed/1/getplayerstatistics`,
  `/statisticsfeed/1/getteamstatistics`, and `/tournaments`. `/` means the
  exact root URL, not a wildcard. The common campaign validator also rejects
  userinfo, non-default ports and fragments.
- Paid work cannot start until the gateway delivers a real Telegram test
  message in the production alert environment and durably revalidates its own
  content-addressed receipt.
- The filtering proxy checks expiry/revocation and durable remaining local
  allowance for every lease and observed stream-byte delta.
- A complete lease allowance is durably escrowed before provider I/O. If a
  write/read, cancellation or durable byte-accounting step becomes uncertain,
  the proxy closes every tunnel, durably revokes the campaign when possible and
  retains the active claim plus all unproven escrow across close and restart;
  it never converts uncertainty into reusable allowance.
- Local reconciliation requires equality between the runner report, request
  ledger, the filter-owned provider-event ledger and campaign state obtained
  through the gateway campaign-control API. Those are correlated copies of a
  proxy-observed counter, not an independent provider invoice.
- Every approval is signed for exactly one `dag_id` and one exact `run_id`.
  Discovery and capture allocations are additionally sealed to one task
  attempt, runner report and request-ledger SHA-256. Replaying the approval in
  another DAG/DagRun or changing a sealed artifact fails closed.
- Approval validity is code-capped at 24 hours from `issued_at`; runtime
  verification rejects future-issued and expired authority. A longer crawl
  cannot turn one HMAC document into durable replay authority.
- No full-crawl approval is usable while the separate full-crawl sentinel is
  false. Any future proposal would require a different release and separately
  reviewed authority; this canary cannot promote itself.

## Deployment prerequisites

Keep the source DAGs paused while preparing a campaign. Do not recreate
SeaweedFS, Trino or any unrelated service for this canary.

Before any paid launch, all of the following external boundaries are required:

- the dedicated PROXYS.IO order `38950`, used only for this paid boundary, with
  a provider-enforced quota of exactly `1.00` decimal GB;
- a fresh, canonical, credential-free provider receipt with `status=active`,
  plan `Bronze`, and `quota_decimal_gb=remaining_decimal_gb=1.00`. Its protected
  screenshot SHA-256, mtime and `observed_at` must agree, and admission rejects
  evidence older than 24 hours or more than five minutes in the future;
- the dedicated credential mounted only into the opt-in
  `whoscored_proxy_filter` service as `WHOSCORED_PROXY_POOL_JSON`, never through the
  common Airflow `proxys.txt`/environment mounts;
- a confidentiality-protected provider leg. The dedicated service dials only
  `pool.proxys.io`, verifies the provider certificate against the system CA and
  the code-owned `pool.infatica.io` TLS name, requires TLS 1.2 or newer and has
  no plaintext fallback;
- the credential-less port-8899 data plane disabled (the shipped production
  default now accepts only authenticated leases; health/control remain usable);
- the shipped `whoscored_paid_gateway`, whose authenticated L7 API exposes only
  bounded fetch, paid-alert preflight and campaign-control operations. It
  validates signed campaign authority and the target/bootstrap/final URL,
  performs a fresh direct Cloudflare check before lease creation, and destroys
  every lease and browser session before returning a credential-free receipt;
- the shipped `flaresolverr_whoscored_paid` instance. Every `/v1` side effect is
  authenticated by a timestamped, one-use HMAC capability bound to the exact
  request body and process instance; it has no host port or direct egress;
- the shipped private Compose topology: scheduler-to-gateway API,
  gateway/browser/filter, direct egress and provider egress are separate
  networks. Neither the paid browser nor the dedicated filter joins `backend`;
- the filtering proxy's pre-provider ClientHello gate: only CONNECT port 443
  with one plaintext SNI exactly matching the approved host may dial the
  provider; missing, malformed, duplicate, IP, ECH/ESNI or mismatched SNI costs
  zero provider requests;
- a schema-v1 deployment attestation with exact `status=ready-v1`, plus
  admission receipts pinning all five protected digest-qualified images, the
  helper-rendered Compose model, mount identities, Docker security settings and
  the paid FlareSolverr extension SHA-256 used by the exact run;
- an immutable release snapshot/image for the scheduler, both FlareSolverr
  services, gateway and filtering proxy. Airflow forked tasks must not inherit
  pre-validation application modules, and a mutable bind-mount hash is not
  proof of loaded code. Use the admitted fresh-exec bootstrap, force-recreate
  each protected process, and require the production FlareSolverr health
  boundary to attest its in-memory `EXTENSION_SHA256` against the approval;
- no unrelated scraper traffic sharing the campaign provider quota.
- the explicit one-shot protected-state initialization in
  `whoscored-production.md`, run from the exact admitted proxy image with no
  network before the first filter service start. The normal Compose command
  never carries `--initialize-whoscored-state`.

Generating or signing a document is preparatory work only. It does not replace
the exact ready deployment attestation, fresh provider receipt, post-create
admission receipts or the DAG's runtime checks.

Authority is deliberately split across three principals:

- The scheduler receives only the exact gateway URL/token, the selected signed
  approval document, and a read-only projection of filter evidence. It has no
  approval HMAC, campaign-ledger HMAC, filter control token, paid-alert HMAC,
  Telegram authority file or writable paid-boundary state.
- `whoscored_paid_gateway` receives the gateway token, filter control token,
  approval HMAC, paid-alert HMAC and FlareSolverr capability secret. It is the
  only principal that reads the paid Telegram target/binding, writes alert
  receipts, verifies alert delivery before fetch, and brokers campaign-control
  operations to the filter.
- `whoscored_proxy_filter` receives the dedicated provider pool, filter control
  token, approval HMAC and campaign-ledger HMAC. It alone mutates the provider
  event and campaign ledgers.

All authority secrets are independently generated and pairwise distinct from
one another and from the generic/SofaScore proxy tokens.

The exact persistent paths are:

- filter-owned campaign state:
  `/opt/airflow/state/whoscored-proxy-filter/whoscored_campaigns.json`;
- filter-owned provider events:
  `/opt/airflow/state/whoscored-proxy-filter/paid_requests.jsonl`;
- filter initialization marker:
  `/opt/airflow/state/whoscored-proxy-filter/.whoscored_state_initialized.json`;
- gateway-owned alert receipts:
  `/opt/airflow/state/whoscored-paid-gateway/alert-receipts`;
- gateway-only read-only alert authority:
  `/opt/airflow/secure/whoscored-alert-authority`.

The scheduler needs:

- `WHOSCORED_PAID_GATEWAY_URL=http://whoscored_paid_gateway:8898` and a distinct
  `WHOSCORED_PAID_GATEWAY_TOKEN`. It receives no filtering-proxy origin, lease
  token or paid browser session identifier;
- `WHOSCORED_PROXY_APPROVAL_HOST_DIR`, an absolute directory outside the
  checkout, mounted read-only into the scheduler at
  `/opt/airflow/secure/whoscored-approvals`;
- `WHOSCORED_PROXY_APPROVAL_ROOT` and the read-only scheduled-pointer root.
  Manual runs pin `approval_id` plus file SHA-256 in `DagRun.conf`; scheduled
  runs use only the immutable run-ID-keyed pointer. No static selector is
  projected into the scheduler;
- the selected regular file owned by Airflow UID `50000`, named
  `<approval_id>.json`, with mode `0600`;
- a read-only bind of `WHOSCORED_PROXY_FILTER_STATE_HOST_DIR` at
  `/opt/airflow/state/whoscored-proxy-filter` for terminal reconciliation;
- a writable immutable `WHOSCORED_OPS_STORE_URI`.

Only the gateway and paid FlareSolverr share
`WHOSCORED_FLARESOLVERR_GATEWAY_SECRET`. Only `whoscored_proxy_filter` receives
`WHOSCORED_PROXY_POOL_JSON`. The scheduler can reach only the gateway L7 API;
it has no direct network path or credential for the filter control plane, paid
browser or provider leg.

The paid path never reads `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` from the
environment or Airflow Variables. Those settings remain best-effort credentials
for ordinary failure callbacks only and cannot satisfy the paid preflight.

Never put an HMAC secret in a command argument, DagRun configuration, log or
approval document. The campaign CLI accepts the separate approval and ledger
HMACs only through protected files or their dedicated environment variables;
neither secret is available to the scheduler.

## 1. Calculate release pins

Run this inside the exact candidate Airflow image/bind mount that will execute
the canary:

```bash
python - <<'PY'
from dags.scripts.whoscored_proxy_runtime import classifier_code_sha256
from scrapers.whoscored.runtime_contract import validate_runtime_contract

print("runtime_sha256=" + validate_runtime_contract()["code_tree_sha256"])
print("classifier_sha256=" + classifier_code_sha256())
PY
```

Any code or runtime-lock change after this step invalidates the approval. Build
a new template instead of editing a signed document. Every scheduler, worker,
task runner and filtering-proxy process must be force-recreated after the final
tree and lock are installed: runtime validation rejects any required file whose
ctime is newer than the kernel-recorded process start. A rolling bind-mount
replacement is therefore an intentional fail-closed outage, not a supported
deployment mode.

## 2. Build an unsigned exact-canary template

Choose unique, non-secret IDs and a UTC validity window of at least eight
hours. Twenty-four hours gives enough room for review without creating a
long-lived authority.

```bash
python scripts/whoscored_proxy_campaign.py template \
  --campaign-id ws-measurement-YYYYMMDD \
  --approval-id ws-measurement-YYYYMMDD-v1 \
  --runtime-sha256 '<runtime SHA-256 from step 1>' \
  --classifier-sha256 '<classifier SHA-256 from step 1>' \
  --issued-at 'YYYY-MM-DDTHH:MM:SS+00:00' \
  --expires-at 'YYYY-MM-DDTHH:MM:SS+00:00' \
  --output /secure/review/ws-measurement-YYYYMMDD-v1.unsigned.json
```

The command writes atomically with mode `0600` and refuses to overwrite an
existing file. Review the complete unsigned JSON. In particular, prove:

- `allowed_dag_ids` is exactly `["dag_canary_whoscored_proxy"]`;
- total and daily budgets are exactly `1_000_000_000`; discovery is exactly
  `250_000_000` and capture is exactly `750_000_000`;
- those legacy canary-envelope fields do not raise the release's independent
  exact `300000000` decimal-byte provider-order lifetime cap;
- request/lease limits are code-owned and exact: discovery permits at most
  `5,000` provider dials across `2,500` URL leases, capture permits at most
  `10,000` dials across `5,000` URL leases, and the signing CLI exposes no
  override. This bounds connection/minimum-charge exposure independently of
  the byte meter while allowing at most one failover dial per lease on average;
- there are exactly two allocations on task `run_whoscored_proxy_canary`:
  `canary-full-history-catalog` / `full-history-catalog` and
  `canary-representative-cohort` / `representative-cohort`;
- discovery paths are exactly `/`, `/Regions`, and `/tournaments`; capture uses
  the complete bounded production path set;
- concurrency does not exceed the two-slot source pool;
- host and path allowlists contain no extra origin or broad path.

The provider contract must additionally state that the dedicated subaccount
has no unbounded per-request, connection or minimum-byte charge. Its
authoritative quota/receipt must cover both the decimal-byte cap and these dial
ceilings; local counters alone do not authorize the canary.

## 3. Sign outside Airflow

Use a protected secret file (mode `0600` or stricter), or inject the neutral
control variable through the operator's secret manager. The following command
does not print the key:

```bash
python scripts/whoscored_proxy_campaign.py sign \
  --input /secure/review/ws-measurement-YYYYMMDD-v1.unsigned.json \
  --output /secure/review/ws-measurement-YYYYMMDD-v1.signed.json \
  --secret-file /secure/secrets/whoscored-approval-hmac \
  --require-exact-canary
sudo install -d -o root -g root -m 0755 \
  "$WHOSCORED_PROXY_APPROVAL_HOST_DIR"
sudo install -o 50000 -g 0 -m 0600 \
  /secure/review/ws-measurement-YYYYMMDD-v1.signed.json \
  "$WHOSCORED_PROXY_APPROVAL_HOST_DIR/ws-measurement-YYYYMMDD-v1.json"
```

Recreate only `airflow-scheduler` so Compose installs the scheduler-only
read-only bind, then verify that the scheduler process sees UID `50000`, mode
`0600`, the exact filename and the pinned SHA-256. Do not expose the host
directory to `airflow-init` or `airflow-webserver`, and do not modify a selected
file after signing.

Verify the mounted bytes and current validity:

```bash
read -r mounted_file_sha256 mounted_approval_sha256 <<EOF
$(docker exec -i airflow-scheduler python - <<'PY'
import hashlib, json, os, stat
from pathlib import Path

from scrapers.whoscored.proxy_campaign import ProxyCampaignApproval

approval_id = os.environ["APPROVAL_ID"]
path = Path(os.environ["WHOSCORED_PROXY_APPROVAL_ROOT"]) / f"{approval_id}.json"
metadata = path.stat(follow_symlinks=False)
assert metadata.st_uid == os.geteuid() == 50000
assert stat.S_IMODE(metadata.st_mode) == 0o600
assert path.name == "ws-measurement-YYYYMMDD-v1.json"
payload = path.read_bytes()
approval = ProxyCampaignApproval.from_dict(json.loads(payload))
approval.verify_digest()
approval.verify_validity()
print(hashlib.sha256(payload).hexdigest(), approval.approval_sha256)
PY
)
EOF
test "$mounted_file_sha256" = '<SHA-256 of the reviewed signed JSON file>'
test "$mounted_approval_sha256" = '<approval_sha256 from sign/verify>'
```

Compare the whole-file digest with `sha256sum` of the reviewed signed JSON and
compare the embedded approval digest with the signing output. This scheduler
check proves only the selected bytes and structural identity. Runtime HMAC
verification happens in the gateway and filter before campaign mutation or
lease creation. The offline signing workspace and key file are never mounted
into the scheduler; the scheduler is outside the approval-HMAC trust domain.

### Bind the gateway-owned paid Telegram destination

Provision a dedicated canonical JSON secret in the gateway-only read-only
authority mount (no trailing newline):

```json
{"bot_token":"123456789:<dedicated-secret>","chat_id":"-1001234567890","schema_version":1}
```

The token prefix is the Telegram bot ID; `chat_id` must be its canonical decimal
integer spelling. Install it as
`$WHOSCORED_PAID_ALERT_AUTHORITY_HOST_DIR/telegram.json` for UID 50000 with mode
`0400`, and keep the root-owned authority directory non-writable and mounted
read-only into `whoscored_paid_gateway` alone. Compose fixes the in-container
path to `/opt/airflow/secure/whoscored-alert-authority/telegram.json`. Do not
reuse the generic callback token.

In the offline signing workspace, bind that bot/chat target to the exact signed
campaign and approval. This is a separate HMAC authority artifact so it does
not modify or weaken the strict proxy-approval schema:

```python
import json
from pathlib import Path

from dags.utils.alerts import paid_alert_target_sha256, sign_paid_alert_binding

approval = json.loads(Path("ws-measurement-YYYYMMDD-v1.json").read_text())
target_sha256 = paid_alert_target_sha256(
    bot_id=123456789,
    chat_id=-1001234567890,
)
binding = sign_paid_alert_binding(
    {
        "schema_version": 1,
        "source": "whoscored",
        "campaign_id": approval["campaign_id"],
        "approval_id": approval["approval_id"],
        "approval_sha256": approval["approval_sha256"],
        "target_sha256": target_sha256,
        "signature_algorithm": "hmac-sha256",
    },
    Path("/secure/secrets/whoscored-paid-alert-hmac").read_text().strip(),
)
Path("telegram-binding.json").write_bytes(
    json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()
)
```

Install the result as
`$WHOSCORED_PAID_ALERT_AUTHORITY_HOST_DIR/binding.json`, UID 50000 and mode
`0400`. Compose fixes its in-container path to
`/opt/airflow/secure/whoscored-alert-authority/binding.json`; neither the
scheduler nor a source runner receives that path, the alert HMAC or the bot
secret. The gateway preflight connects directly to Telegram without ambient
HTTP proxy variables and embeds the exact run identity plus a fresh 128-bit
delivery nonce. It
accepts HTTP 200 only when Telegram returns JSON `ok: true`, the exact bot
`from.id`/`is_bot`, signed `result.chat.id`, exact rendered message text, a
positive integer `message_id`, and a `date` inside the bounded
request/response time window.
It then writes one content-addressed receipt below
`/opt/airflow/state/whoscored-paid-gateway/alert-receipts` and durably records
the delivered identity. The scheduler and source runner receive only bounded,
non-secret receipt metadata; they do not open the gateway state. Every
`POST /v1/fetch` makes the gateway reopen and cryptographically revalidate the
receipt, current target/binding, campaign and approval before a proxy lease can
be claimed. Secret rotation, path replacement, target changes, receipt
tampering, message-contract changes, replay or cross-campaign reuse fail closed
and require a new real preflight.

## 4. Trigger the paused canary

Keep the DAG paused until the exact `ready-v1` deployment attestation, fresh
provider quota receipt, per-service post-create admission receipts, approval,
gateway-owned alert authority and source pool have all been independently
reviewed. Re-run admission if the provider receipt has aged past its 24-hour
window. The DagRun ID is signed into the approval and must be exactly
`manual__<campaign-id>`. Only then may an operator unpause the canary and create
one manual run:

```bash
airflow dags trigger dag_canary_whoscored_proxy \
  --run-id 'manual__ws-measurement-YYYYMMDD' --conf '{
  "transport_policy": "direct_then_paid",
  "paid_approval_id": "ws-measurement-YYYYMMDD-v1",
  "paid_approval_sha256": "<approval_sha256 from sign/verify>"
}'
```

Do not put target counts or a byte cap in DagRun configuration. Population
upper bounds are stored with an explicit, immutable basis. They are never
misrepresented as verified remaining raw misses and therefore cannot produce a
full-crawl cap.

The task order is deliberately strict:

1. validate structural ID/SHA pins, release hashes, exact canary shape, expiry
   and two-slot pool, then require the gateway to authenticate the HMAC-bound
   campaign and return its active snapshot;
2. ask the gateway to deliver a real production Telegram preflight alert and
   persist/revalidate its immutable campaign/approval/target/message receipt;
3. execute full-history catalog discovery under the separately capped
   `250_000_000`-byte allocation, then durably seal its report/request ledger;
4. freeze the release-bound representative scopes
   `ENG-Premier League=2526` and `INT-World Cup=2026` from the immutable
   full-history catalog and Bronze frontier; require at least 100 completed
   matches and 100 roster players and at most 90 estimated work items;
5. execute that cohort under the separately capped `750_000_000`-byte capture
   allocation (at most 100 backfill work items), using raw cache and direct
   transport before paid fallback, then seal it;
6. reconcile the runner report, request ledger, filter-owned provider-event
   ledger and gateway-authenticated campaign state exactly, complete both
   allocations, and seal the campaign before persisting measurement evidence;
7. expose one terminal measurement outcome gate.

Both runner phases must finish with `status=success` and return code zero for
the result to be eligible for a separately reviewed full-budget proposal. A
sealed, exactly reconciled but incomplete/retryable measurement is retained as
`measurement_recorded_non_authorizing`; fatal execution, an unsealed allocation
or any accounting mismatch still fails the terminal gate.

## 5. Read immutable evidence

The completion XCom contains only a bounded artifact reference. Detailed
evidence is stored under the operational-store prefix:

```text
proxy-campaigns/<campaign-id>/canary/<dagrun-id>/
```

The content-addressed measurement records:

- the exact signed local cap and proxy-observed provider-leg bytes;
- exactly reconciled local accounting totals and the sealed campaign snapshot;
- report and request-ledger SHA-256 identities;
- direct/raw/paid route counts and wire bytes;
- provider-byte sample count, p50, p95 and maximum for each URL workload class;
- immutable target-count basis and provenance;
- the exact full-budget expression and proposed cap when every required class
  has a positive target and at least 20 observations.

The formula is evaluated with integer arithmetic:

```text
ceil(sum(remaining_targets[class] * p95_provider_bytes[class]) * 1.25)
```

Every required class (`catalog_or_bootstrap`, `stage_data`, `stage_feed`,
`statistics_feed`, `match_live`, `match_preview`, and `player_profile`) must
have a positive remaining target and at least 20 paid observations. Otherwise
the artifact is retained as incomplete evidence, no full cap is proposed and
the terminal task succeeds only with
`status=measurement_recorded_non_authorizing` and
`full_approval_eligible=false`. Never replace a missing p95 with zero or an
average. The frozen representative cohort also requires at least 20 stages;
direct/cache hits can still leave a class undersampled and therefore
non-authorizing. Full-population/capacity upper bounds have
`target_basis=full-population-upper-bound-v1`, produce
`status=unverified_remaining_targets`, and can never yield a proposed cap. A
future verified inventory must subtract integrity-valid raw objects and use
`target_basis=verified-raw-miss-inventory-v1`. A zero-spend canary also fails.
Even when a positive cap is proposed, it is measurement evidence only. The
full-crawl sentinel remains false, so do not create or attempt a full-backfill
approval under this release.

## Emergency stop and rollback

Pause the canary DAG first, then durably revoke the exact mounted approval:

```bash
docker exec airflow-scheduler \
  airflow dags pause dag_canary_whoscored_proxy
approval_id='ws-measurement-YYYYMMDD-v1'
approval_host_path="$WHOSCORED_PROXY_APPROVAL_HOST_DIR/$approval_id.json"
campaign_ledger_host_path="$WHOSCORED_PROXY_FILTER_STATE_HOST_DIR/whoscored_campaigns.json"
test -f "$approval_host_path"
test -f "$campaign_ledger_host_path"
python scripts/whoscored_proxy_campaign.py revoke \
  --approval "$approval_host_path" \
  --approval-id "$approval_id" \
  --approval-sha256 '<pinned approval SHA-256>' \
  --ledger "$campaign_ledger_host_path" \
  --reason 'operator emergency stop: <ticket/reference>' \
  --output "/secure/revocations/$approval_id.json" \
  --secret-file /secure/secrets/whoscored-approval-hmac \
  --ledger-secret-file /secure/secrets/whoscored-ledger-hmac
```

The revocation receipt is atomic and mode `0600`. Revocation blocks all new
claims and preserves active claims plus their escrow as authenticated forensic
evidence; it cannot be undone by retry, continuation or a UTC-day rollover. Let
in-flight tasks fail; do not delete raw objects, backfill plans, request
ledgers, campaign ledgers or immutable evidence. Keep the source pool at two
slots. The operator-only HMAC files above are never copied into the scheduler.
Recovery uses a newly reviewed approval ID, never the revoked file.

Expiry is also a hard stop. Extending validity or increasing a cap requires a
new signed approval and a separate review; mutable environment scalars and
DagRun booleans cannot top up a campaign.
