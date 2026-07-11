# SofaScore tournament registry

`tournaments.json` is the single source of truth for SofaScore tournament IDs,
navigation slugs, source season IDs, and activation state.

Refresh every discoverable football tournament and its seasons with:

```bash
make sofascore-discovery
```

The one-shot uses the production Airflow image for `curl_cffi`, calls only the
direct SofaScore JSON API, and mounts only this directory writable. It never
loads a browser or a paid proxy. `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, and
the repository proxy file are disabled at the libcurl transport layer. A 403
or exhausted retry budget fails closed and leaves the registry untouched.

For a read-only drift check (exit 2 means the registry would change):

```bash
make sofascore-discovery-check
```

Discovery and activation are separate. New records always receive
`canonical_id: null` and `enabled: false`. To activate one, first map it to an
existing canonical competition, make sure its canonical seasons exist in
`configs/medallion/competitions.yaml`, and only then set `enabled: true`.
Airflow mounts this registry read-only and fails DAG import before any paid
scrape if an active mapping is incomplete or its scheduled season is missing.
