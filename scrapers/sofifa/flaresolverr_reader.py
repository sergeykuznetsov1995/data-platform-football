"""
SoFIFA reader that fetches pages through FlareSolverr instead of Selenium.

soccerdata 1.9.0 ships with seleniumbase + undetected-chromedriver for SoFIFA
(PR #932), but the resulting headless driver does not click the Cloudflare
Turnstile checkbox automatically — sofifa.com keeps returning the challenge
HTML even after 5x retries (verified 2026-05-12). FlareSolverr (Camoufox-based,
already used by WhoScored events) passes the Turnstile reliably, so this
subclass keeps all of soccerdata's SoFIFA parsing intact and only swaps the
HTTP transport layer.

Wire up via env var ``FLARESOLVERR_URL`` (default ``http://flaresolverr:8191``)
or the ``flaresolverr_url`` kwarg on ``SoFIFAScraper``.
"""
from __future__ import annotations

import html as html_module
import io
import logging
import os
import re
import uuid
from pathlib import Path
from typing import IO, Iterable, List, Optional, Union

import soccerdata as sd

from scrapers.base.flaresolverr_client import (
    FlareSolverrCFChallengeFailed,
    FlareSolverrClient,
    FlareSolverrTabCrashed,
)

logger = logging.getLogger(__name__)

# Recreate the FlareSolverr session every N requests to dodge an internal
# Chromium tab crash on sofifa.com SPA pages (FS v3.4.6 / Chrome 142). The
# crash is not docker-OOM (~150MiB usage at crash), it's Chromium internal —
# observed between request #3 and #10 of a session, 2026-05-13. 4 sits below
# the observed lower bound so rotation almost always lands before crash;
# rotation cost is ~15s per cycle (fresh CF challenge per session = ~12s +
# bootstrap fetch). Player_ratings (~545 requests) was failing at 8 because
# the [1..7] window inside every session was wide enough to hit the crash.
SESSION_RECREATE_EVERY = 4

_PRE_BODY_RE = re.compile(r"<pre[^>]*>(.*?)</pre>", re.DOTALL | re.IGNORECASE)

#: Main-6 FIFA card aggregates. sofifa renders these client-side from inline JS
#: (``POINT_PAC=64,POINT_SHO=44,...``) — they are NOT present as DOM text, so we
#: read them straight out of the page source.
_MAIN6_JS = {
    'pace': 'PAC', 'shooting': 'SHO', 'passing': 'PAS',
    'dribbling': 'DRI', 'defending': 'DEF', 'physical': 'PHY',
}


def _eur_to_int(text: Optional[str]) -> Optional[int]:
    """Parse a sofifa money string ('€104M', '€250K', '€0') to an int of euros."""
    if not text:
        return None
    m = re.search(r"€\s*([\d.]+)\s*([MK]?)", text)
    if not m:
        return None
    mult = {'M': 1_000_000, 'K': 1_000, '': 1}[m.group(2)]
    return int(round(float(m.group(1)) * mult))


def _extract_card_extras(raw_html: str, tree) -> dict:
    """Pull the issue-#42 fields the upstream score loop does not capture.

    Covers the main-6 card aggregates (JS), market value / wage / release clause,
    contract dates, and the profile header (position / dob / height / weight /
    nationality). All fields degrade to ``None`` when absent so a partial page
    never aborts the run.
    """
    out: dict = {}
    for col, ab in _MAIN6_JS.items():
        m = re.search(rf"POINT_{ab}=(\d+)", raw_html)
        out[col] = int(m.group(1)) if m else None

    root = tree.getroot()
    body = root.text_content()
    mv = re.search(r"(€[\d.]+[MK]?)\s*Value", body)
    mw = re.search(r"(€[\d.]+[MK]?)\s*Wage", body)
    mrc = re.search(r"Release clause\s*(€[\d.]+[MK]?)", body)
    out['value_eur'] = _eur_to_int(mv.group(1)) if mv else None
    out['wage_eur'] = _eur_to_int(mw.group(1)) if mw else None
    out['release_clause_eur'] = _eur_to_int(mrc.group(1)) if mrc else None

    mc = re.search(r"Contract valid until\s*(\d{4})", body)
    mj = re.search(r"Joined\s*([A-Z][a-z]{2} \d{1,2}, \d{4})", body)
    out['contract_valid_until'] = int(mc.group(1)) if mc else None
    out['joined'] = mj.group(1) if mj else None

    prof = root.xpath("//div[contains(@class,'profile')]")
    if prof:
        ptxt = ' '.join(prof[0].text_content().split())
        mp = re.search(r"\b([A-Z]{2,3}(?:,\s*[A-Z]{2,3})*)\b\s+\d+y\.o\.", ptxt)
        md = re.search(r"\(([A-Z][a-z]{2} \d{1,2}, \d{4})\)", ptxt)
        mh = re.search(r"(\d{2,3})cm", ptxt)
        mwt = re.search(r"(\d{2,3})kg", ptxt)
        out['position'] = mp.group(1) if mp else None
        out['dob'] = md.group(1) if md else None
        out['height_cm'] = int(mh.group(1)) if mh else None
        out['weight_kg'] = int(mwt.group(1)) if mwt else None
    else:
        out.update(position=None, dob=None, height_cm=None, weight_kg=None)

    # Nationality is the <img title="..."> inside the profile flag link
    # (<a href="/players?na=NN"><img title="Brazil" .../></a>). The <a> itself
    # carries no title, so target the inner image.
    flags = root.xpath(
        "//div[contains(@class,'profile')]"
        "//a[contains(@href,'/players?na=')]/img/@title"
    )
    out['nationality'] = flags[0] if flags else None
    return out


def _force_english(url: str) -> str:
    """Append ``hl=en-US`` to sofifa.com URLs so the page comes back in English.

    sofifa.com geolocates the request and serves the page in the local language
    of the egress IP (e.g. Dutch from EU datacenter IPs). soccerdata's
    ``read_player_ratings`` parses scores by searching for English text labels
    ("Overall rating", "Dribbling", ...) via XPath ``contains(text, ...)``,
    so a non-English page yields ~all-NULL attribute columns. The ``hl=``
    query parameter overrides geo-locale; an ``<link rel="alternate"
    hreflang="en" href="...?hl=en-US">`` in the page itself documents the API.
    """
    if 'hl=' in url:
        return url
    sep = '&' if '?' in url else '?'
    return f"{url}{sep}hl=en-US"


def _extract_pre_body(html: str) -> str:
    """Extract JSON body from FlareSolverr's <pre>-wrapped HTML response.

    FlareSolverr renders all responses through Chromium, which displays raw
    JSON inside `<html><body><pre>{...}</pre></body></html>`. soccerdata's
    SoFIFA reader expects the cached file to be valid JSON, so for `.json`
    filepaths we strip the wrapper and HTML-unescape the body.
    """
    match = _PRE_BODY_RE.search(html)
    if match:
        return html_module.unescape(match.group(1))
    return html  # already plain (no wrapper) — pass through


class FlareSolverrSoFIFAReader(sd.SoFIFA):
    """soccerdata SoFIFA reader that fetches HTML via FlareSolverr."""

    def __init__(
        self,
        flaresolverr_url: str = "http://flaresolverr:8191",
        proxy: Optional[str] = None,
        max_timeout_ms: int = 90_000,
        versions: Union[str, int, List[int]] = "latest",
        no_cache: bool = False,
        no_store: bool = False,
        data_dir: Optional[Path] = None,
        leagues: Optional[Union[str, List[str]]] = None,
        session_recreate_every: int = SESSION_RECREATE_EVERY,
    ):
        # FlareSolverr session must exist BEFORE sd.SoFIFA.__init__ runs,
        # because that constructor calls self.read_versions() which goes
        # through our _download_and_save right away.
        self._fs_client = FlareSolverrClient(url=flaresolverr_url)
        self._session_id = self._new_session_id()
        self._max_timeout_ms = max_timeout_ms
        # When PROXY_FILTER_URL is set, route the FlareSolverr session through the
        # ad-tech filtering proxy (#652) — it holds the residential creds and rotates
        # the upstream itself, so we pass a static credential-free URL. Used for both
        # the initial session and every _maybe_recreate_session.
        self._proxy_url = os.environ.get("PROXY_FILTER_URL") or proxy
        self._request_count = 0
        self._session_recreate_every = session_recreate_every
        self._session_closed = False
        self._fs_client.create_session(self._session_id, proxy_url=self._proxy_url)
        logger.info("FlareSolverr session %s created", self._session_id)

        kw = dict(
            versions=versions,
            no_cache=no_cache,
            no_store=no_store,
            proxy=None,  # proxy now lives inside the FlareSolverr session
            leagues=leagues,
        )
        if data_dir is not None:
            kw["data_dir"] = data_dir
        super().__init__(**kw)

    @staticmethod
    def _new_session_id() -> str:
        return f"sofifa-{uuid.uuid4().hex[:8]}"

    @classmethod
    def _all_leagues(cls) -> dict[str, str]:
        """Delegate league lookup to ``sd.SoFIFA``.

        soccerdata's ``BaseReader._all_leagues`` keys ``LEAGUE_DICT`` by
        ``cls.__name__``. Our subclass would search for the literal
        ``'FlareSolverrSoFIFAReader'`` and find nothing — every league would
        be rejected as invalid. Forward the call to the canonical parent.
        """
        return sd.SoFIFA._all_leagues()

    def _init_webdriver(self):
        # Selenium not needed — FlareSolverr owns the browser.
        return None

    def read_versions(self, max_age=1) -> "pd.DataFrame":
        """Override ``soccerdata.SoFIFA.read_versions`` for the post-EA-FC DOM.

        Upstream (``soccerdata/sofifa.py``) parses two nested dropdowns at
        ``//header/section/p/select[1|2]/option``. The EA SPORTS FC rebrand
        moved the pickers to ``<select id="select-version">`` (one option per
        FIFA/FC edition, ``value`` carries ``r=<latest update id>``) and
        ``<select id="select-roster">`` (one option per update of the *current*
        edition, ``value`` carries ``r=<update id>``, text = release date). The
        old xpath now matches 0 options → empty ``versions`` →
        ``set_index('version_id')`` raises ``KeyError`` in the constructor,
        killing all SoFIFA scraping (#650, silent-fail sibling of #647).

        One homepage request (matches the wrapper contract): one row per
        edition with its latest update id, enriched with the release date from
        the roster picker where the current edition's ids overlap.
        """
        SO_FIFA_API = "https://sofifa.com"
        filepath = self.data_dir / "index.html"
        reader = self.get(SO_FIFA_API, filepath, max_age)
        page = reader.read()
        if isinstance(page, bytes):
            page = page.decode("utf-8", "replace")
        return self._parse_versions(page)

    @staticmethod
    def _parse_versions(page: str) -> "pd.DataFrame":
        """Parse the SoFIFA homepage edition/roster pickers into a versions frame.

        Pure (no I/O) so it is unit-testable against a saved page. Returns a
        DataFrame indexed by ``version_id`` with ``fifa_edition`` + ``update``
        columns — same contract as upstream, so ``versions='latest'`` (which
        takes ``.tail(1)`` = max id) and downstream readers keep working.
        """
        import pandas as pd
        from lxml import html as _html

        tree = _html.fromstring(page)

        def _rid(value: Optional[str]) -> Optional[int]:
            m = re.search(r"[rR]=(\d+)", value or "")
            return int(m.group(1)) if m else None

        # roster picker → {update id: release date} for the current edition only
        update_date: dict[int, str] = {}
        for opt in tree.xpath('//select[@id="select-roster"]/option'):
            rid = _rid(opt.get("value"))
            if rid is not None:
                update_date[rid] = opt.text_content().strip()

        # version picker → one row per edition (value = its latest update id)
        rows: List[dict] = []
        for opt in tree.xpath('//select[@id="select-version"]/option'):
            rid = _rid(opt.get("value"))
            if rid is None:
                continue
            edition = opt.text_content().strip()
            rows.append({
                "version_id": rid,
                "fifa_edition": edition,
                "update": update_date.get(rid, edition),
            })

        if not rows:
            raise ValueError(
                "SoFIFA read_versions: 0 editions parsed from "
                "select#select-version — DOM drifted again (#650)"
            )
        return (
            pd.DataFrame(rows)
            .drop_duplicates("version_id")
            .set_index("version_id")
            .sort_index()
        )

    def read_player_ratings(self, team=None, player=None) -> "pd.DataFrame":
        """Override soccerdata.SoFIFA.read_player_ratings to inject ``player_id``.

        Upstream returns a DataFrame keyed by player NAME and sorted by name,
        which makes merging back to ``player_id`` unreliable: rating-page
        names ("James Philip Milner") often differ from catalogue names
        ("James Milner"). We replicate the upstream loop verbatim and add
        ``player_id`` to the per-row dict so it propagates as a real column.

        Source mirror: ``soccerdata/sofifa.py:375-493``.
        """
        import pandas as pd
        from itertools import product
        from lxml import html as _html
        from soccerdata._common import standardize_colnames

        SO_FIFA_API = "https://sofifa.com"
        urlmask = SO_FIFA_API + "/player/{}/?r={}&set=true"
        filemask = "player_{}_{}.html"

        if player is None:
            players = self.read_players(team=team).index.unique()
        elif isinstance(player, int):
            players = [player]
        else:
            players = player

        score_labels = [
            "Overall rating", "Potential", "Crossing", "Finishing",
            "Heading accuracy", "Short passing", "Volleys", "Dribbling",
            "Curve", "FK Accuracy", "Long passing", "Ball control",
            "Acceleration", "Sprint speed", "Agility", "Reactions",
            "Balance", "Shot power", "Jumping", "Stamina", "Strength",
            "Long shots", "Aggression", "Interceptions", "Attack position",
            "Vision", "Penalties", "Composure", "Defensive awareness",
            "Standing tackle", "Sliding tackle",
            "GK Diving", "GK Handling", "GK Kicking",
            "GK Positioning", "GK Reflexes",
        ]

        ratings: list[dict] = []
        iterator = list(product(self.versions.iterrows(), players))
        for i, ((version_id, version), pid) in enumerate(iterator):
            logger.info(
                "[%s/%s] Retrieving ratings for player ID %s in %s edition",
                i + 1, len(iterator), pid, version["update"],
            )
            filepath = self.data_dir / filemask.format(pid, version_id)
            url = urlmask.format(pid, version_id)
            raw_bytes = self.get(url, filepath).read()
            raw_text = raw_bytes.decode("utf-8", "replace")
            tree = _html.parse(
                io.BytesIO(raw_bytes), parser=_html.HTMLParser(encoding="utf8")
            )
            node_player_name = tree.xpath("//div[contains(@class, 'profile')]/h1")
            if not node_player_name:
                logger.warning("player %s: no profile h1 found, skipping", pid)
                continue
            node = node_player_name[0]
            before_br = node.xpath("string(./text()[1])").strip()
            after_br = node.xpath("string(./br/following-sibling::text()[1])").strip()
            scores: dict = {
                "player_id": int(pid),
                "player": before_br if before_br else after_br,
                **version.to_dict(),
            }
            for s in score_labels:
                value = None
                for xpath in (
                    f"//p[.//text()[contains(.,'{s}')]]/span/em",
                    f"//div[contains(.,'{s}')]/em",
                    f"//li[not(self::script)][.//text()[contains(.,'{s}')]]/em",
                ):
                    nodes = tree.xpath(xpath)
                    if nodes:
                        value = nodes[0].text.strip()
                        break
                # The detailed "Dribbling" skill collides with the main-6 card
                # aggregate (also 'dribbling'); keep both under distinct names.
                if s == 'Dribbling':
                    key = 'dribbling_detail'
                elif s == 'Attack position':
                    # SoFIFA labels the attacking attribute "Attack position";
                    # upstream searches "Positioning" and silently captures
                    # "GK Positioning" instead (#316). Store under a key that
                    # standardize_colnames maps back to 'positioning'.
                    key = 'Positioning'
                else:
                    key = s
                scores[key] = value
            # Issue #42: main-6 aggregates, market value / wage, contract dates,
            # and profile header (position / dob / height / weight / nationality).
            scores.update(_extract_card_extras(raw_text, tree))
            ratings.append(scores)

        return (
            pd.DataFrame(ratings)
            .pipe(standardize_colnames)
            .set_index(["player"])
            .sort_index()
        )

    def read_team_ratings(self) -> "pd.DataFrame":
        """Override soccerdata.SoFIFA.read_team_ratings to drop dead FC-26 cols.

        Upstream requests 23 rating columns via ``&showCol[]=`` and parses each
        from a ``<td data-col='...'>`` cell. EA removed the team-tactics block
        (build-up / chance-creation / defence sliders), international &
        domestic prestige, and the ``whole_team_average_age`` label from the
        FC 26 team page, so sofifa.com no longer renders those 15 cells —
        soccerdata silently emits them as all-NULL (issue #601; confirmed
        live 2026-06-16 via audit_bronze_columns.py). We keep only the 8 columns
        that still exist on the page and request exactly those, so the URL is
        honest and Bronze stops carrying 15 dead columns.

        Source mirror: ``soccerdata/sofifa.py:287-373`` (same loop, trimmed dict).
        """
        import pandas as pd
        from itertools import product
        from lxml import html as _html
        from soccerdata._common import safe_xpath_text
        from soccerdata._config import TEAMNAME_REPLACEMENTS

        SO_FIFA_API = "https://sofifa.com"

        # Only the rating cells sofifa.com still renders on the FC 26 team page.
        ratings = {
            "oa": "overall",
            "at": "attack",
            "md": "midfield",
            "df": "defence",
            "tb": "transfer_budget",
            "cw": "club_worth",
            "ps": "players",
            "sa": "starting_xi_average_age",
        }

        urlmask = SO_FIFA_API + "/teams?lg={}&r={}&set=true"
        for rating_id in ratings:
            urlmask += f"&showCol[]={rating_id}"
        filemask = "teams_{}_{}.html"

        leagues = self.read_leagues()

        teams: list[dict] = []
        iterator = list(product(leagues.iterrows(), self.versions.iterrows()))
        for i, ((lkey, league), (version_id, version)) in enumerate(iterator):
            logger.info(
                "[%s/%s] Retrieving team ratings for %s in %s edition",
                i + 1, len(iterator), lkey, version["update"],
            )
            league_id = league["league_id"]
            filepath = self.data_dir / filemask.format(league_id, version_id)
            url = urlmask.format(league_id, version_id)
            reader = self.get(url, filepath)

            # Explicit utf-8 (mirrors read_player_ratings): the team page carries
            # € money cells (transfer_budget / club_worth); without the hint lxml
            # falls back to latin-1 and mojibakes the euro sign.
            tree = _html.parse(reader, parser=_html.HTMLParser(encoding="utf8"))
            for node in tree.xpath("//table/tbody/tr"):
                teams.append(
                    {
                        "league": lkey,
                        "team": node.xpath(".//td[2]//a")[0].text,
                        **{
                            desc: safe_xpath_text(
                                node,
                                f".//td[@data-col='{key}']//text()",
                                warn=f"Could not parse {desc} ({key}) stat.",
                            )
                            for key, desc in ratings.items()
                        },
                        **version.to_dict(),
                    }
                )

        return (
            pd.DataFrame(teams)
            .replace({"team": TEAMNAME_REPLACEMENTS})
            .set_index(["league", "team"])
            .sort_index()
        )

    def _maybe_recreate_session(self) -> None:
        if self._request_count <= 0:
            return
        if self._request_count % self._session_recreate_every != 0:
            return
        self._rotate_session(reason=f"scheduled after {self._request_count} requests")

    def _rotate_session(self, reason: str) -> None:
        """Destroy current session and create a fresh one. Idempotent on errors."""
        old = self._session_id
        try:
            self._fs_client.destroy_session(old)
        except Exception as e:
            logger.warning("destroy_session(%s) before rotation failed: %s", old, e)
        self._session_id = self._new_session_id()
        self._fs_client.create_session(self._session_id, proxy_url=self._proxy_url)
        logger.info("Rotated FlareSolverr session %s -> %s (%s)", old, self._session_id, reason)

    # Max consecutive crash/timeout retries before propagating the error.
    # 3 covers the observed worst-case of two crashes in a row during long
    # player_ratings iteration; higher values risk infinite loops on dead URLs.
    _max_recoveries: int = 3

    def _fs_get_with_recovery(self, url: str) -> dict:
        """Call ``fs_client.get(url)`` with session rotation on tab crash / CF timeout."""
        import time
        attempt = 0
        while True:
            try:
                return self._fs_client.get(
                    url,
                    self._session_id,
                    max_timeout_ms=self._max_timeout_ms,
                )
            except (FlareSolverrTabCrashed, FlareSolverrCFChallengeFailed) as e:
                attempt += 1
                if attempt > self._max_recoveries:
                    logger.error("Recovery exhausted (%d attempts) for %s: %s", attempt, url, e)
                    raise
                reason = "tab crash" if isinstance(e, FlareSolverrTabCrashed) else "CF challenge timeout"
                # Linear backoff (3s, 6s, 9s) — gives FS Chromium time to
                # release the crashed tab's memory before we hit it again.
                backoff = 3.0 * attempt
                logger.warning(
                    "%s on %s (attempt %d/%d), sleeping %.1fs then rotating session: %s",
                    reason, url, attempt, self._max_recoveries, backoff, e,
                )
                time.sleep(backoff)
                self._rotate_session(reason=f"{reason} attempt {attempt}")

    def _download_and_save(
        self,
        url: str,
        filepath: Optional[Path] = None,
        var: Optional[Union[str, Iterable[str]]] = None,
    ) -> IO[bytes]:
        if var is not None:
            raise NotImplementedError(
                "FlareSolverrSoFIFAReader does not support JS variable extraction "
                "(used by FBref's get_page, never by SoFIFA)."
            )

        self._maybe_recreate_session()
        url = _force_english(url)

        # Retry on tab crash or CF challenge timeout with rotation: Chromium
        # 142 crashes the tab unpredictably on sofifa.com SPA pages even at
        # low memory; sometimes a fresh session's Turnstile challenge also
        # times out after 90s. In both cases recovery is the same — destroy
        # session + create fresh one. We retry up to ``_max_recoveries``
        # times because the first retry can crash again (observed 2026-05-13:
        # ~2 consecutive crashes during long player_ratings iteration). The
        # backoff between retries lets FlareSolverr's old Chromium release
        # memory before we hit it again. soccerdata caches successful HTML
        # on disk so the previous request progress is not lost.
        response = self._fs_get_with_recovery(url)

        html = response.get("html") or ""
        status = response.get("status", 0)

        if "_cf_chl_opt" in html or "Just a moment" in html:
            raise FlareSolverrCFChallengeFailed(
                f"Cloudflare challenge HTML returned for {url} (status={status})"
            )
        if not html:
            raise ConnectionError(f"FlareSolverr returned empty body for {url}")

        # FlareSolverr always returns rendered HTML, even for JSON endpoints
        # (it wraps the JSON body inside `<html><body><pre>...</pre></body></html>`).
        # soccerdata.SoFIFA.read_leagues() calls `json.load(filepath)` on the
        # cached file, so for `.json` filepaths we must strip the wrapper.
        if filepath is not None and str(filepath).endswith(".json"):
            body = _extract_pre_body(html).encode("utf-8")
        else:
            body = html.encode("utf-8")

        if not self.no_store and filepath is not None:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            with filepath.open(mode="wb") as fh:
                fh.write(body)

        self._request_count += 1
        return io.BytesIO(body)

    def _validate_page(self, url: str) -> str:
        # Defensive: in BaseSeleniumReader this is called after driver.get(url).
        # Our overridden _download_and_save bypasses the driver path, so this
        # should never run. Fail loud rather than NPE on self._driver.
        raise RuntimeError(
            "_validate_page should not be called in FlareSolverrSoFIFAReader "
            f"(url={url})"
        )

    def close(self) -> None:
        if self._session_closed:
            return
        try:
            self._fs_client.destroy_session(self._session_id)
            logger.info("Destroyed FlareSolverr session %s", self._session_id)
        except Exception as e:
            logger.warning("destroy_session(%s) at close failed: %s", self._session_id, e)
        finally:
            self._session_closed = True

    def __enter__(self) -> "FlareSolverrSoFIFAReader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
