# ESPN configuration

## Denominator registry (`denominator.tsv`, #1499)

`denominator.tsv` is the one file that says which ESPN soccer leagues count as
ours. ESPN has no age or level field, so every league we know of gets a class,
and only adult men's official leagues form the target. The file also keeps the
leagues we do not collect (women, youth, friendlies…), so that daily discovery
of new leagues does not report them again. Loader:
`scrapers/espn/denominator.py` (`load_denominator()`; env
`ESPN_DENOMINATOR_PATH` overrides the path). Any format error fails closed.

Tab-separated, UTF-8, sorted by `slug`. Columns:

| Column | Meaning |
|---|---|
| `slug` | ESPN league slug, the key (equals bronze `competition_slug`) |
| `espn_id` | ESPN numeric league id (empty if not recorded) |
| `name` | ESPN league name |
| `espn_gender` | ESPN's raw `gender` (`MALE`, `FEMALE`, `UNKNOWN`; `FEMALE?` — guessed from the slug, detail never fetched) |
| `class` | one of the classes below |
| `class_reason` | `rule:<rule>` from the class rule or `manual:<text>` from a manual table |
| `in_target` | `1` exactly when `class = senior_official` and `hidden = 0`; the loader refuses anything else |
| `hidden` | `1` for a league ESPN serves by slug but does not list (`sui.1`): senior, outside the percentage, checked by hand weekly |
| `live` | `1` when the slug is in the core list `leagues?limit=500` of 24.09.2026 |
| `current_season_year` | ESPN `source_season_year` snapshot of 31.07/24.09; the live value is taken by #1501 |
| `deep_level` | empty until #1513 |
| `fotmob_id`, `sofascore_id` | empty; filled by hand when cross-source ids are unfrozen |
| `note` | free text from the 24.09 review |

### Classes and history-queue priority

| `class` | Priority | Meaning |
|---|---|---|
| `senior_official` | 1 | adult men's official competition — the target |
| `youth`, `olympic`, `friendly`, `reserve` | 9 | outside the percentage, history last |
| `women`, `college` | 0 | not collected |

The denominator of the freshness percentage is counted in matches by its
consumers (#1505); a target league without matches contributes nothing.

### Class rule

`scrapers/espn/classify.py`, in this order:

1. `MEN_OVERRIDE` (manual) → `senior_official`: `concacaf.champions_cup`, which
   ESPN marks `gender=FEMALE` by mistake;
2. `women`: `gender=FEMALE` (`rule:women_gender`), `WOMEN_RE` on the slug
   (`rule:women_slug`), or the word `women` in the name (`rule:women_name`);
3. `college`: slug `usa.ncaa*`;
4. `olympic`: `olympic` in the slug or name;
5. `youth`: `YOUTH_RE` (U15–U23) on the slug or name;
6. `friendly`: `fifa.friendly`, `club.friendly`, `nonfifa`, `friendly.*`, or
   `friendly` in the name (`rule:friendly`); then the manual `INVITATIONAL`
   table of preseason/invitational events (`esp.joan_gamper`,
   `jpn.world_challenge`, `fifa.intercontinental.cup`, `bangabandhu.cup`);
7. `reserve`: `reserve`, `II`, `.b`/`_b` suffix;
8. otherwise `senior_official` (`rule:default`). `eng.trophy`, `usa.open` and
   `bra.camp.*` stay in the target.

`tests/unit/scrapers/test_espn_classify.py` runs the rule over the recorded
catalog and fails if the file and the rule disagree.

### Adding or changing a league

Add or edit one row: `class` and `class_reason` from `classify()` (or a
`manual:` reason plus an entry in the manual table of `classify.py`), and
`in_target`/`hidden`/`live` consistent with it. New leagues come from
`scrapers/espn/catalog_core.py`: `diff_catalog()` against the core list, then
`propose_row()` over the league detail gives the candidate row.

Check:

```bash
pytest tests/unit/configs/test_espn_denominator_file.py \
  tests/unit/scrapers/test_espn_classify.py tests/unit/scrapers/test_espn_catalog_core.py -q
```

## Transport policy (`transport_policy.json`, #1500)

One gate per VM (`scrapers/espn/gate.py`, `TransportGate`) decides where and
when every ESPN request goes. Its state is one JSON file under `flock`
(`ESPN_GATE_STATE_PATH`, default `${AIRFLOW_HOME:-/opt/airflow}/state/espn/gate.json`),
so all processes share pace, origin blocks and counters. Loader:
`load_transport_policy()`; an unknown key, non-increasing steps, a share
outside (0, 1) or missing lanes fail closed.

| Key | Meaning |
|---|---|
| `clusters` | `site`: primary `site.web.api.espn.com`, reserve `site.api.espn.com` (closed by Akamai for our User-Agent since 04.08); `core`: `sports.core.api.espn.com`, no reserve |
| `origin_block_seconds`, `origin_probe_seconds` | a 403 closes an origin for 30 min; then exactly one request probes it every 30 min |
| `reserve_probe_seconds` | the reserve starts closed and is probed at most once a day |
| `all_blocked_pause_seconds`, `all_blocked_probe_seconds` | no open origin in a cluster: 30 min pause, then one probe every 5 min; `history` freezes first and reopens last |
| `steps` | pace ladder S0…S3, requests per minute; the ceiling is `ESPN_GATE_STEP_CEILING` (default 0 = S0; raising it is #1510) |
| `live_share` | share of each minute reserved for `live`: `history` is admitted only while its permits of the last minute stay below `1 - live_share` of the step |
| `lanes` | per-lane daily request fuse (`daily_requests`), set above the S3 maximum; UTC date. Bytes are counted per lane (state `daily`) but never capped: no daily MB ceilings (roadmap assumption 4) |
| `reset` | auto-reset: a 429, ≥ 3 × 403 in 60 s, or 5xx+timeouts > 2 % of ≥ 50 requests in 5 min → one step down, 15 min cooldown with `history` frozen, then back to the ceiling; two resets within an hour → S0 for 6 h and an `alert` in the state file |
| `uncompressed_warn_bytes` | an `identity` response larger than this is logged as a warning |

A 403 is never retried on another origin inside the same request: the request
fails with `OriginBlocked` (`AllOriginsBlocked` when the cluster has no open
origin), is journaled as `blocked_deferred`, and the caller retries it in a
later wave.
