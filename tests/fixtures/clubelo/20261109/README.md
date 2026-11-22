# ClubElo /Fixtures, captured 2026-11-09 (#2279)

`Fixtures.html.gz` — the gzip body stored by the daily run in
`bronze.clubelo_raw_page` (`page='/Fixtures'`, rating_date 2026-11-08,
fetched 2026-11-09 20:30:58 UTC, wire 55 211 B, sha256
c793f4294c22fd717521c6031bc81b4563b0185af4e457aad8089464cbbe42e4),
exported unchanged. The main table holds 50 matches; game 1700437 carries
the bounded-percent `>99.9` in the home `chance` stake cell (wide layout
`min1081` span, narrow `>99`, very narrow `%`) that fail-closed the day
(`F3 chance: not a percent or '<bound': '>99.9>99%'`). The same bound on
2026-11-04 hit game 1700440 away `|loss`; 2026-11-10 the same game/position
as here. All three raw pages stay outside the repo (offline reparse evidence:
`.local/tasks/clubelo-2279-2280/`); this one page is the regression fixture.
