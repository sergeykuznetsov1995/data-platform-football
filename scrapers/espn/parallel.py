"""Bounded fetch scheduling; callers alone publish results and query Trino."""
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import time

WORKERS = (1, 2, 4, 8)


def protected(snapshot, now):
    return (any(snapshot.get('all_blocked', {}).values()) or
            any(now < snapshot.get(key, 0) for key in
                ('hold_until', 'cooldown_until', 'history_frozen_until')))


def interruptible_sleep(seconds, check, *, sleep=time.sleep, monotonic=time.monotonic):
    end = monotonic() + seconds
    while True:
        check()
        left = end - monotonic()
        if left <= 0:
            return
        sleep(min(left, 0.25))


def bounded_fetch(items, fetch, *, limit, check, client_factory, tick=lambda: None):
    """Yield (item, result, error). Never queue more than the current limit.

    Stop/check errors stop submission and drain siblings. A failed match is
    yielded independently. Client state is confined to one slot at a time;
    all clients are drained/closed before generator exit, including exceptions.
    """
    clients, pending = {}, {}
    iterator = iter(items)
    exhausted = False
    stopped = None
    pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix='espn-history')
    try:
        while pending or not exhausted:
            try:
                tick()
                check()
                cap = limit()
                if type(cap) is not int or cap not in WORKERS:
                    raise ValueError('invalid history worker limit')
            except BaseException as exc:
                stopped = stopped or exc
                exhausted = True
                cap = 0
            while not exhausted and len(pending) < cap:
                try:
                    check()
                    if len(pending) >= limit():
                        break
                    item = next(iterator)
                except StopIteration:
                    exhausted = True
                    break
                except BaseException as exc:
                    stopped = stopped or exc
                    exhausted = True
                    break
                busy = {slot for slot, _ in pending.values()}
                slot = next(n for n in range(8) if n not in busy)
                if slot not in clients:
                    clients[slot] = client_factory(slot)
                pending[pool.submit(fetch, clients[slot], item)] = (slot, item)
            if not pending:
                break
            ready, _ = wait(pending, timeout=0.25, return_when=FIRST_COMPLETED)
            for future in ready:
                _, item = pending.pop(future)
                try:
                    result = future.result()
                except BaseException as exc:
                    yield item, None, exc
                else:
                    yield item, result, None
        if stopped is not None:
            raise stopped
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
        for client in clients.values():
            client.flush()
