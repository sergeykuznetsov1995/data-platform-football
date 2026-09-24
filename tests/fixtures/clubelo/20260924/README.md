# ClubElo site fixtures, captured 2026-09-24

Pages of clubelo.com captured on **2026-09-24, 15:25–15:58 UTC** (times from the `date`
response header, or from file mtime where headers were not saved — see `manifest.json`),
plus two header-only responses re-captured live on 2026-09-24 18:43 UTC (#1461).
The site's rating date on all pages is **2026-09-22** (`<h1><a href="/2026-09-22/...">`).

Source: review of epic #1459, register findings M-11, R-12, R-42
(`/root/clubelo-review-20260924/REGISTER.md` on the VM, comments in #1459).
Host copy of the raw samples (outside the repo): `/root/clubelo-archive/site-samples-20260924/`.

## Files

| File | URL | Status | Wire | Notes |
|---|---|---|---|---|
| `Ranking.html.gz` | `/Ranking` | 200 | gzip 88 609 B (br 85 930 B, identity 935 456 B) | `eloData = [` (1741 rows), `var vegaJson`, accordion of 96 sections |
| `Results.html.gz` | `/Results` | 200 | gzip 46 210 B (identity 567 507 B) | 63 result rows, 2026-09-20..22 |
| `Fixtures.html.gz` | `/Fixtures` | 200 | gzip 40 756 B (identity 504 738 B) | fixtures table is **empty** |
| `club_Arsenal.html.gz` | `http://clubelo.com/Arsenal/` → 301 → `/Arsenal` | 200 | br, bytes **not measured** | saved decoded (560 222 B), **gzipped by us** (`gzip -9 -n`); 220 vega points |
| `club_riverplate.html.gz` | `/riverplate` | 200 | gzip 48 980 B | match-table rows with 6 cells (no result) |
| `club_santos-fc_2.html.gz` | `/santos-fc_2` | 200 | gzip 48 937 B | duplicate match on 2026-09-02 |
| `club_lsapi-4199.html.gz` | `/lsapi-4199` | 200 | gzip 44 442 B | new club, 30 vega points (2026-01-21..2026-09-13) |
| `login.html.gz` | `/login/` | 200 | not measured | saved decoded (4 230 B), **gzipped by us**; "registration is not available yet." |
| `Arsenal_Results.500.html` | `/Arsenal/Results` | 500 | 145 B plain | "Server Error (500)" |
| `lsapi-2483.302.headers.txt` | `/lsapi-2483` | 302 | — | headers only, `location: /` (dead club slug redirects to home) |
| `Arsenal_Results.500.headers.txt` | `/Arsenal/Results` | 500 | — | headers only, `cf-cache-status: BYPASS` |
| `*.headers.txt` | — | — | — | response headers as saved by `curl -D`; `set-cookie` lines (`__cf_bm` tokens) removed |

`manifest.json` holds, per file: url, http_status, captured_at (UTC), content_encoding_on_wire,
wire_bytes, identity_bytes, sha256, gzip_by (`wire` = the bytes are exactly what the network
sent; `us` = we compressed a decoded copy), headers_file, notes.

## What is missing

- Response headers of the club pages (`riverplate`, `santos-fc_2`, `lsapi-4199`) and of `/login/`
  were not saved; their `captured_at` is the file mtime.
- `content-length` is absent from every saved header set (HTTP/2 / chunked responses) — size on
  the wire is the byte size of the wire-gzip file.
- Wire bytes of `/Arsenal` (br) and `/login/` were not measured.
- The 302 and 500 headers were not saved on 2026-09-24 15:xx; they were re-captured the same day
  at 18:43 UTC with `curl -A python-requests/2.32 -H 'Accept-Encoding: gzip'` (2 requests).

## Rules

- These are frozen samples of the site layout; do not refresh them in place — add a new dated
  directory instead.
- The review parser prototype (`proto_parse.py`) stays outside the repo; it is only a reference
  for the tests of the parser tasks (#1462, #1463).
- `tests/unit/scrapers/test_clubelo_fixtures.py` checks the manifest (existence, sha256),
  the total size (≤ 1 MB) and the layout markers of every page.
