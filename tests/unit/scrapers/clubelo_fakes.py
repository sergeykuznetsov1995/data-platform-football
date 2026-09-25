"""Shared fakes for the ClubElo history tests (#1462): HTTP from fixtures, store in memory."""

from __future__ import annotations

import gzip
from pathlib import Path
from typing import Callable, Dict, List, Union

import requests
from requests.structures import CaseInsensitiveDict

FIXTURE_DIR = Path(__file__).parents[2] / "fixtures" / "clubelo" / "20260924"


def fixture_html(name: str) -> str:
    with gzip.open(FIXTURE_DIR / name, "rt", encoding="utf-8") as fh:
        return fh.read()


class FakeRaw:
    def __init__(self, wire: bytes) -> None:
        self.wire = wire
        self.decode_flags: List[bool] = []

    def read(self, decode_content: bool = True) -> bytes:
        self.decode_flags.append(decode_content)
        return self.wire


class FakeResponse:
    def __init__(self, status: int, wire: bytes = b"", headers: Dict[str, str] = None) -> None:
        self.status_code = status
        self.headers = CaseInsensitiveDict(headers or {})
        self.raw = FakeRaw(wire)
        self.closed = False

    def close(self) -> None:
        self.closed = True


def gzip_response(html: str, status: int = 200) -> FakeResponse:
    return FakeResponse(status, gzip.compress(html.encode("utf-8"), mtime=0),
                        {"content-encoding": "gzip"})


def fixture_response(name: str) -> FakeResponse:
    """A fixture page as the wire sent it (gzip)."""

    return FakeResponse(200, (FIXTURE_DIR / name).read_bytes(), {"content-encoding": "gzip"})


def redirect_response() -> FakeResponse:
    return FakeResponse(302, b"", {"location": "/"})


Answer = Union[FakeResponse, Exception, Callable[[], FakeResponse]]


class FakeSession:
    """Answers by URL path; a list is consumed one answer per request."""

    def __init__(self, answers: Dict[str, Union[Answer, List[Answer]]]) -> None:
        self.answers = answers
        self.calls: List[Dict] = []

    def get(self, url, **kwargs):
        path = url.split("clubelo.com", 1)[1]
        self.calls.append({"path": path, **kwargs})
        answer = self.answers[path]
        if isinstance(answer, list):
            answer = answer.pop(0)
        if callable(answer) and not isinstance(answer, FakeResponse):
            answer = answer()
        if isinstance(answer, Exception):
            raise answer
        return answer


class MemoryStore:
    """In-memory stand-in for ``IcebergHistoryStore`` (same four methods)."""

    def __init__(self, manifest: List[Dict] = None) -> None:
        self.tables: Dict[str, List[Dict]] = {}
        self.appends: List[str] = []
        self.ensured = False
        if manifest:
            self.tables["clubelo_history_manifest"] = list(manifest)

    def ensure_tables(self) -> None:
        self.ensured = True

    def known_slugs(self):
        return {row["slug"] for row in self.tables.get("clubelo_history_manifest", [])}

    def closed_slugs(self):
        from scrapers.clubelo.history import closed_from_manifest

        return closed_from_manifest(self.tables.get("clubelo_history_manifest", []))

    def append(self, table: str, rows: List[Dict]) -> None:
        if rows:
            self.appends.append(table)
            self.tables.setdefault(table, []).extend(rows)

    def rows(self, table: str) -> List[Dict]:
        return self.tables.get(table, [])


def no_sleep(_seconds: float) -> None:
    return None


def network_error() -> Exception:
    return requests.ConnectionError("connection reset")
