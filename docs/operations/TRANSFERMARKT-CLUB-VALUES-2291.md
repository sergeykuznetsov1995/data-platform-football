# #2291 club-value source contract v1

## Authorized evidence

Owner authorized bounded live capture before implementation. Four logical requests, no retries and no backfill were made on 2026-11-20 UTC through the existing Transfermarkt gateway. Three competition pages returned HTTP 200; `/marktwertverlauf/verein/{club_id}` for the first current club returned HTTP 404 and is not a v1 source.

Fixtures are public `VIE1` competition pages, not player-MV sums:

- current: SHA256 `e25bae93bbb0d5e756ff2843d196e05b35c22adddb37c0ae5850ca59c5cb193b`;
- `stichtag=2024-06-01`: SHA256 `5a93a91f6bb6a404d77c7a46c15383f696524493227e647bd0ebca94c44e47e8`;
- `stichtag=2013-04-15`: SHA256 `1a1b99b9c678a3e99a8453ce21c6643d5c6c400cbf56b992063f38d332db7f9e`.

Raw manifests, bodies, request outcomes and traffic are under `capture/`. Raw readback confirmed URL/body hash and HTTP status for all four captures.

## Source shape

Use `/{competition_slug}/marktwerteverein/{route}/{competition_id}` for current and the same path plus `/stichtag/{YYYY-MM-DD}` for history. Route is `wettbewerb` or `pokalwettbewerb`, matching the registry.

The first `table.items` is authoritative for club identity and table values. Required headers are `#`, `wappen`, `Club`, `League`, `Value {date}`, `Current value`, `%`. Club ID/slug/name come from the club link, not from an image or player rows. Table `-` is `explicit_none`, never numeric zero.

`select[name=stichtag] option` is the available-date universe for this competition. Dates are unique ISO values. Full available history requires a capture at the earliest option; a later `stichtag` is only a bounded range.

The Highcharts config supplies `xAxis.categories` and `series`. Categories and every series' points are right-aligned to the latest published date. The table's `Current value` anchors that alignment. A series shorter than categories has no points at earlier dates. Categories must be unique, ascending and included in the options.

Series names must uniquely match table club names. Crest IDs, when present, must agree with the table club ID. Duplicate/conflicting identities, missing final values, unknown money locale and malformed JS fail closed. Table money is display-rounded while graph values are exact. Representations are merged only when the graph value rounds to the exact table display precision. Real captures also contain genuine table/graph differences: Ha Noi FC `4.95M` versus `4.975M`, Bac Ninh FC `1.25M` versus `1.275M`. These remain separate observations marked `value_differs`; neither is discarded or silently corrected.

## Point and observation semantics

- Positive Highcharts `y`: `observed_present`, amount from the same point.
- Highcharts `y=0`: `unknown`, `value_amount=NULL`; the graph does not prove a real zero.
- Table `-`: `explicit_none`, `value_amount=NULL`.
- Table current is merged with the final graph point only after display-precision amount/status validation. Table previous is reconciled with a graph point at `stichtag` when present; otherwise it is an additional table point. Differing representations become separate `value_differs` observations with distinct source point IDs.
- Currency v1 is EUR, parsed from table money with the existing exact money parser. Other currencies fail closed rather than being mislabeled.
- `source_point_id = competition_id:club_id:value_date:currency`. Observation natural key adds `observed_at`. Repeated capture of the same point is idempotent; a changed later observation is a correction, not an overwrite of history.
- Missing tables, malformed rows, duplicate categories/series/clubs, graph/table conflicts and empty layout are errors, not `observed_empty`.

Every row retains club/competition/date/currency, raw value, source point kind, raw capture ID/body hash/URL, captured/extracted/observed timestamps, parser/schema/scope/run/cycle/universe and batch lineage. Parser does not read or sum player market values.

## Writer and DQ

The isolated writer accepts only a structurally valid extraction. Merge key is `(club_id, value_date, observed_at, source_point_id)`. Per-club receipt contains expected point/status counts and a physical hash. Physical readback compares the complete club set; the receipt is reread. No DAG, schedule, production grant, backfill or Silver/Gold change is enabled by this implementation.

## Remaining live proof

Full universe coverage and the earliest-date capture remain unproven until separately authorized runs. Weekly scheduled continuation, three green DQ windows and production acceptance are outside the bounded capture authorization.
