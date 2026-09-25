"""ESPN raw write path: two PUTs, no HEAD, background queue (#1500)."""

from __future__ import annotations

import threading

import pytest
from pyarrow import fs

from scrapers.espn.raw_store import EspnRawStore, RawStoreError, RawWriteQueue
from scrapers.espn.transport_contracts import EndpointType, canonicalize_target

URL = "https://site.web.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary"


class CountingFs:
    """Object-store stand-in over pyarrow's mock filesystem, counting calls."""

    def __init__(self, fail=False, gate=None):
        self.inner = fs._MockFileSystem()
        self.calls = {"get_file_info": 0, "open_output_stream": 0, "create_dir": 0}
        self.fail = fail
        self.gate = gate

    def get_file_info(self, path):
        self.calls["get_file_info"] += 1
        return self.inner.get_file_info(path)

    def open_output_stream(self, path, compression=None):
        self.calls["open_output_stream"] += 1
        if self.gate is not None:
            self.gate.wait(5)
        if self.fail:
            raise OSError("s3 unavailable")
        try:
            return self.inner.open_output_stream(path, compression=compression)
        except OSError:
            self.inner.create_dir(path.rsplit("/", 1)[0], recursive=True)
            return self.inner.open_output_stream(path, compression=compression)

    def create_dir(self, path, recursive=True):
        self.calls["create_dir"] += 1
        return self.inner.create_dir(path, recursive=recursive)

    def open_input_file(self, path):
        return self.inner.open_input_file(path)


def _store(filesystem):
    return EspnRawStore(filesystem, "bucket/espn", uri_prefix="s3://bucket/espn")


@pytest.mark.unit
def test_store_of_new_summary_is_two_puts_and_no_head():
    counting = CountingFs()
    store = _store(counting)
    target = canonicalize_target(URL, {"event": 740880})
    store.store(target, EndpointType.SUMMARY, b'{"header":{}}')
    assert counting.calls == {
        "get_file_info": 0,
        "open_output_stream": 2,
        "create_dir": 0,
    }
    # Same body again: still two idempotent PUTs, no read-back.
    store.store(target, EndpointType.SUMMARY, b'{"header":{}}')
    assert counting.calls["open_output_stream"] == 4
    assert counting.calls["get_file_info"] == 0
    assert store.load(target)[0] == b'{"header":{}}'


@pytest.mark.unit
def test_queue_put_does_not_wait_and_flush_waits_for_the_write():
    release = threading.Event()
    counting = CountingFs(gate=release)
    queue = RawWriteQueue(_store(counting))
    target = canonicalize_target(URL, {"event": 740881})
    record = queue.put(target, EndpointType.SUMMARY, b"{}")  # returns at once
    assert record.raw_uri.startswith("s3://bucket/espn/blobs/")
    assert queue.pending(target) == (b"{}", record)
    release.set()
    queue.flush()
    assert queue.pending(target) is None
    assert queue.store.load(target)[0] == b"{}"
    queue.close()


@pytest.mark.unit
def test_queue_write_error_surfaces_on_flush():
    queue = RawWriteQueue(_store(CountingFs(fail=True)))
    target = canonicalize_target(URL, {"event": 740882})
    queue.put(target, EndpointType.SUMMARY, b"{}")
    with pytest.raises(RawStoreError):
        queue.flush()
    queue.flush()  # the error is reported once
    queue.close()


@pytest.mark.unit
def test_local_store_still_finds_blob_by_alias(tmp_path):
    store = EspnRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target(URL, {"event": 740883})
    record = store.store(target, EndpointType.SUMMARY, b'{"a":1}')
    body, loaded = store.load(target)
    assert body == b'{"a":1}' and loaded.content_hash == record.content_hash
