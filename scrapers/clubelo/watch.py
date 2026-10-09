"""Weekly registration/Fixtures observations; no API switch or data writes (#1466).

Only meaningful page states are compared. HTTP/parser failures keep the last
good observation, and Telegram failures keep the notification pending. The
state lives on the existing Airflow logs volume, outside delivered source.
"""

from __future__ import annotations

import fcntl
import html
import json
import logging
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from lxml import html as lxml_html

from scrapers.clubelo.parse import LayoutChanged, page_h1
from scrapers.clubelo.transport import ClubEloBlocked, ClubEloTransport, get_source

logger = logging.getLogger(__name__)
STATE_FILE = "/opt/airflow/logs/clubelo_registration_watch.json"
_REGISTRATION = re.compile(r"register|registration|sign[ -]?up|create account", re.I)
_PRICING = re.compile(r"pricing|prices|subscription", re.I)
_PRICE = re.compile(r"[$€£]\s*\d+(?:[.,]\d+)*|\d+(?:[.,]\d+)*\s*(?:[$€£]|(?:USD|EUR|GBP)\b)", re.I)
_HIDDEN_STYLE = re.compile(r"(?:display\s*:\s*none|visibility\s*:\s*hidden)\b", re.I)


def _text(element) -> str:
    return " ".join(element.text_content().split())


def parse_login(body: bytes) -> dict:
    """Recognise a login page before interpreting a missing closed marker."""
    # Login has no charset meta; byte parsing would treat UTF-8 € as Latin-1.
    text = body.decode("utf-8")
    # lxml repairs truncated identity responses. Missing tail is unknown,
    # never evidence that the registration warning disappeared.
    if not re.search(r'</body\s*>\s*</html\s*>\s*$', text, re.I):
        raise LayoutChanged("watch /login/: incomplete document")
    doc = lxml_html.fromstring(text)
    for node in doc.xpath('//script|//style|//noscript|//*[@hidden]|//*[@style]'):
        if (node.tag in {"script", "style", "noscript"} or node.get('hidden') is not None
                or _HIDDEN_STYLE.search(node.get('style', ''))):
            if node.getparent() is not None:
                node.drop_tree()
    titles = doc.xpath('//title')
    headings = doc.xpath('//h1|//h2')
    if not (titles and "clubelo" in _text(titles[0]).lower()
            and any(_text(h).lower() in {"login", "log in", "sign in"} for h in headings)):
        raise LayoutChanged("watch /login/: login page markers missing")
    bodies = doc.xpath('//body')
    if not bodies:
        raise LayoutChanged("watch /login/: body missing")
    text = _text(bodies[0])
    registration_links, pricing_links = set(), set()
    for link in bodies[0].xpath('.//a[@href]'):
        url = urlsplit(link.get('href'))
        if url.scheme not in ("", "http", "https") or not url.path:
            continue
        # Query/fragment may carry session tokens. They are never state keys.
        target = (url.netloc.lower() + url.path) if url.netloc else url.path
        evidence = _text(link) + " " + url.path
        if _REGISTRATION.search(evidence):
            registration_links.add(target)
        if _PRICING.search(evidence):
            pricing_links.add(target)
    return {
        "registration_unavailable": "account registration is not available yet" in text.lower(),
        "registration_links": sorted(registration_links),
        "pricing_links": sorted(pricing_links),
        "prices": sorted({" ".join(m.group().split()).upper() for m in _PRICE.finditer(text)}),
    }


def parse_fixtures(body: bytes) -> dict:
    """Ignore the club accordion: only rows in the main Fixtures block count."""
    doc = lxml_html.fromstring(body.decode("utf-8"))
    _, page = page_h1(body.decode("utf-8"))
    main = doc.xpath('//div[contains(concat(" ",normalize-space(@class)," ")," blatt ")]')
    sidebar = doc.xpath('//div[contains(concat(" ",normalize-space(@class)," ")," stamm ")]')
    if page != "Fixtures" or len(main) != 1 or not sidebar:
        raise LayoutChanged("watch /Fixtures: page markers missing")
    rows = [row for row in main[0].xpath('.//tr[td]')
            if sum(bool(_text(cell)) for cell in row.xpath('./td')) >= 2]
    return {"has_rows": bool(rows)}


def _interesting(key: str, value: dict) -> bool:
    if key == "fixtures":
        return value["has_rows"]
    return (not value["registration_unavailable"] or bool(value["registration_links"])
            or bool(value["pricing_links"]) or bool(value["prices"]))


def _load(path: Path) -> dict:
    if not path.exists():
        return {"version": 1, "observed": {}, "notified": {}}
    state = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(state, dict) or state.get("version") != 1:
        raise ValueError("watch state: unsupported format")
    for field in ("observed", "notified"):
        values = state.get(field)
        if not isinstance(values, dict) or set(values) - {"login", "fixtures"}:
            raise ValueError(f"watch state: invalid {field}")
        for key, value in values.items():
            if not isinstance(value, dict):
                raise ValueError(f"watch state: invalid {key}")
            if key == "fixtures":
                valid = set(value) == {"has_rows"} and type(value["has_rows"]) is bool
            else:
                valid = (set(value) == {"registration_unavailable", "registration_links", "pricing_links", "prices"}
                         and type(value["registration_unavailable"]) is bool
                         and all(isinstance(value[k], list) and all(isinstance(v, str) for v in value[k])
                                 for k in ("registration_links", "pricing_links", "prices")))
            if not valid:
                raise ValueError(f"watch state: invalid {key}")
    return state


def _save(path: Path, state: dict) -> None:
    """Replace in the same filesystem; never expose a partially written JSON."""
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(state, stream, ensure_ascii=False, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def run_watch(transport, *, state_file=STATE_FILE, notifier, source="html") -> dict:
    """Observe both pages with one counted transport; return errors, never raise.

    A separate lock file survives atomic state replacement. Concurrent/manual
    invocations do not race Telegram dedup; a busy lock returns a warning.
    """
    result = {"status": "success", "errors": [], "notified": False}
    path = Path(state_file)
    checked_at = datetime.now(timezone.utc).isoformat()
    try:
        get_source(source)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.with_name(path.name + ".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            state = _load(path)
            for key, page, parser in (("login", "/login/", parse_login),
                                      ("fixtures", "/Fixtures", parse_fixtures)):
                try:
                    answer = transport.get(page)
                    if answer.status != 200:
                        raise LayoutChanged(f"{page}: HTTP {answer.status}")
                    value = parser(answer.body)
                    state["observed"][key] = value
                    # Initial closed/empty observations are a silent baseline.
                    if key not in state["notified"] and not _interesting(key, value):
                        state["notified"][key] = value
                except ClubEloBlocked as exc:
                    result["errors"].append(str(exc))
                    break  # No more requests after a source block.
                except Exception as exc:
                    result["errors"].append(f"{page}: {type(exc).__name__}: {exc}")
            state["checked_at"] = checked_at
            state["errors"] = list(result["errors"])
            # Persist observations BEFORE sending: unwritable state means no TG.
            _save(path, state)
            changed = {k: v for k, v in state["observed"].items()
                       if state["notified"].get(k) != v}
            if changed:
                lines = ["ClubElo: изменилось состояние сайта"]
                if 'login' in changed:
                    login = changed['login']
                    lines.append("Регистрация пока недоступна." if login['registration_unavailable']
                                 else "Предупреждение о закрытой регистрации исчезло.")
                    previous = state['notified'].get('login', {})
                    for field, label in (('registration_links', 'Ссылки регистрации'),
                                         ('prices', 'Видимые цены'),
                                         ('pricing_links', 'Ссылки цен/подписки')):
                        if login[field]:
                            lines.append(label + ': ' + ', '.join(login[field]))
                        elif previous.get(field):
                            lines.append(label + ': больше не обнаружены.')
                if 'fixtures' in changed:
                    lines.append("Fixtures: появились строки матчей." if changed['fixtures']['has_rows']
                                 else "Fixtures: строк матчей нет.")
                lines.extend(["https://clubelo.com/login/", "https://clubelo.com/Fixtures",
                              "Переход на платный API требует отдельного решения владельца."])
                message = html.escape('\n'.join(lines))
                if notifier(message):
                    state["notified"] = dict(state["observed"])
                    state["notified_at"] = checked_at
                    _save(path, state)
                    result["notified"] = True
                else:
                    result["errors"].append("Telegram did not accept the notification")
                    state["errors"] = list(result["errors"])
                    _save(path, state)
            result["observed"] = state["observed"]
    except Exception as exc:
        result["errors"].append(f"{type(exc).__name__}: {exc}")
    result.update(checked_at=checked_at, wire_bytes=transport.wire_bytes, requests=transport.requests)
    if result["errors"]:
        result["status"] = "error"
        logger.warning("ClubElo watch: %s", result)
    else:
        logger.info("ClubElo watch: %s", result)
    return result


def run_default(*, state_file=STATE_FILE, source="html") -> dict:
    import requests
    from utils.alerts import send_telegram_message

    with requests.Session() as session:
        return run_watch(ClubEloTransport(session), state_file=state_file,
                         notifier=send_telegram_message, source=source)
